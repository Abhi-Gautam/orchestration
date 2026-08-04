package activities

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	orchestrationv1 "orchestration/gen/orchestration/v1"
)

const (
	GenerateArtifactActivityName   = "GenerateArtifactActivity"
	AggregateArtifactsActivityName = "AggregateArtifactsActivity"
	maxStoredArtifactBytes         = 1 << 20
)

type ArtifactStoreConfig struct {
	Endpoint  string
	Region    string
	AccessKey string
	SecretKey string
	Bucket    string
}

type ArtifactActivities struct {
	client *s3.Client
	bucket string
	store  string
}

type GenerateArtifactInput struct {
	ActivityVersion string
	WorkDuration    time.Duration
	FailureCase     orchestrationv1.ReusableArtifactFailureCase
}

type AggregateArtifactsInput struct {
	References      []*orchestrationv1.ArtifactReference
	ActivityVersion string
	InjectFailure   bool
}

type ReportSummary struct {
	ArtifactCount  int32
	SemanticDigest string
}

type storedArtifact struct {
	Namespace       string    `json:"namespace"`
	WorkflowID      string    `json:"workflowId"`
	ActivityID      string    `json:"activityId"`
	ActivityType    string    `json:"activityType"`
	ActivityVersion string    `json:"activityVersion"`
	Content         string    `json:"content"`
	PublishedAt     time.Time `json:"publishedAt"`
}

func NewArtifactActivities(ctx context.Context, config ArtifactStoreConfig) (*ArtifactActivities, error) {
	if strings.TrimSpace(config.Endpoint) == "" || strings.TrimSpace(config.Region) == "" ||
		strings.TrimSpace(config.AccessKey) == "" || strings.TrimSpace(config.SecretKey) == "" ||
		strings.TrimSpace(config.Bucket) == "" {
		return nil, errors.New("RustFS artifact store configuration is incomplete")
	}

	client := s3.NewFromConfig(aws.Config{
		Region: config.Region,
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(
			config.AccessKey,
			config.SecretKey,
			"",
		)),
	}, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(config.Endpoint)
		options.UsePathStyle = true
	})

	activities := &ArtifactActivities{
		client: client,
		bucket: config.Bucket,
		store:  fmt.Sprintf("rustfs://%s", config.Bucket),
	}
	if err := activities.ensureBucket(ctx); err != nil {
		return nil, err
	}
	return activities, nil
}

func (a *ArtifactActivities) GenerateArtifactActivity(ctx context.Context, input GenerateArtifactInput) (*orchestrationv1.ArtifactReference, error) {
	info := activity.GetInfo(ctx)
	if strings.TrimSpace(input.ActivityVersion) == "" {
		return nil, temporal.NewNonRetryableApplicationError(
			"activity version is required",
			"InvalidReusableArtifactInput",
			nil,
		)
	}
	if input.WorkDuration <= 0 {
		return nil, temporal.NewNonRetryableApplicationError(
			"heavy-work duration must be positive",
			"InvalidReusableArtifactInput",
			nil,
		)
	}

	objectKey := reusableArtifactKey(
		info.Namespace,
		info.WorkflowExecution.ID,
		info.ActivityID,
		info.ActivityType.Name,
		input.ActivityVersion,
	)
	reference := &orchestrationv1.ArtifactReference{Store: a.store, ObjectKey: objectKey}
	logger := activity.GetLogger(ctx)

	exists, err := a.objectExists(ctx, objectKey)
	if err != nil {
		return nil, fmt.Errorf("check reusable artifact %q: %w", objectKey, err)
	}
	if exists {
		logger.Info("GenerateArtifactActivity reused artifact",
			"activityId", info.ActivityID,
			"attempt", info.Attempt,
			"objectKey", objectKey,
		)
		return reference, nil
	}

	logger.Info("GenerateArtifactActivity performing heavy work",
		"activityId", info.ActivityID,
		"attempt", info.Attempt,
		"activityVersion", input.ActivityVersion,
		"duration", input.WorkDuration,
		"objectKey", objectKey,
	)
	if err := performArtifactWork(ctx, input.WorkDuration, info.Attempt); err != nil {
		return nil, err
	}

	if info.Attempt == 1 && input.FailureCase == orchestrationv1.ReusableArtifactFailureCase_REUSABLE_ARTIFACT_FAILURE_CASE_BEFORE_PUBLICATION {
		return nil, temporal.NewApplicationError(
			"injected failure before artifact publication",
			"InjectedBeforeArtifactPublication",
			info.ActivityID,
			info.Attempt,
		)
	}

	payload, err := json.Marshal(storedArtifact{
		Namespace:       info.Namespace,
		WorkflowID:      info.WorkflowExecution.ID,
		ActivityID:      info.ActivityID,
		ActivityType:    info.ActivityType.Name,
		ActivityVersion: input.ActivityVersion,
		Content:         semanticArtifactContent(info.WorkflowExecution.ID, info.ActivityID, input.ActivityVersion),
		PublishedAt:     time.Now().UTC(),
	})
	if err != nil {
		return nil, fmt.Errorf("encode artifact: %w", err)
	}
	if _, err := a.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(a.bucket),
		Key:         aws.String(objectKey),
		Body:        bytes.NewReader(payload),
		ContentType: aws.String("application/json"),
	}); err != nil {
		return nil, fmt.Errorf("publish reusable artifact %q: %w", objectKey, err)
	}

	logger.Info("GenerateArtifactActivity published artifact",
		"activityId", info.ActivityID,
		"attempt", info.Attempt,
		"objectKey", objectKey,
	)
	if info.Attempt == 1 && input.FailureCase == orchestrationv1.ReusableArtifactFailureCase_REUSABLE_ARTIFACT_FAILURE_CASE_AFTER_PUBLICATION {
		return nil, temporal.NewApplicationError(
			"injected failure after artifact publication",
			"InjectedAfterArtifactPublication",
			info.ActivityID,
			info.Attempt,
		)
	}

	return reference, nil
}

func (a *ArtifactActivities) AggregateArtifactsActivity(ctx context.Context, input AggregateArtifactsInput) (ReportSummary, error) {
	info := activity.GetInfo(ctx)
	if len(input.References) != 5 {
		return ReportSummary{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("exactly five artifact references are required, got %d", len(input.References)),
			"InvalidArtifactAggregationInput",
			nil,
		)
	}
	if strings.TrimSpace(input.ActivityVersion) == "" {
		return ReportSummary{}, temporal.NewNonRetryableApplicationError(
			"activity version is required",
			"InvalidArtifactAggregationInput",
			nil,
		)
	}

	digest := sha256.New()
	for index, reference := range input.References {
		expectedActivityID := fmt.Sprintf("artifact-%03d", index)
		artifact, err := a.readStoredArtifact(ctx, reference)
		if err != nil {
			return ReportSummary{}, err
		}
		if artifact.Namespace != info.Namespace ||
			artifact.WorkflowID != info.WorkflowExecution.ID ||
			artifact.ActivityID != expectedActivityID ||
			artifact.ActivityType != GenerateArtifactActivityName ||
			artifact.ActivityVersion != input.ActivityVersion ||
			strings.TrimSpace(artifact.Content) == "" {
			return ReportSummary{}, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("artifact %q does not match the expected semantic identity", reference.GetObjectKey()),
				"InvalidReusableArtifact",
				nil,
				reference.GetObjectKey(),
				expectedActivityID,
			)
		}
		_, _ = fmt.Fprintf(digest, "%d\x00%s\x00%s\x00%s\x00", index, artifact.ActivityID, artifact.ActivityVersion, artifact.Content)
	}

	summary := ReportSummary{
		ArtifactCount:  int32(len(input.References)),
		SemanticDigest: hex.EncodeToString(digest.Sum(nil)),
	}
	if info.Attempt == 1 && input.InjectFailure {
		return ReportSummary{}, temporal.NewApplicationError(
			"injected retryable failure after artifact aggregation",
			"InjectedArtifactAggregationFailure",
			info.Attempt,
			summary.SemanticDigest,
		)
	}

	activity.GetLogger(ctx).Info("AggregateArtifactsActivity consumed artifacts",
		"artifactCount", summary.ArtifactCount,
		"semanticDigest", summary.SemanticDigest,
		"attempt", info.Attempt,
	)
	return summary, nil
}

func (a *ArtifactActivities) readStoredArtifact(ctx context.Context, reference *orchestrationv1.ArtifactReference) (storedArtifact, error) {
	if reference == nil || reference.GetStore() != a.store || strings.TrimSpace(reference.GetObjectKey()) == "" {
		return storedArtifact{}, temporal.NewNonRetryableApplicationError(
			"artifact reference is invalid or belongs to an unsupported store",
			"InvalidArtifactReference",
			nil,
		)
	}

	response, err := a.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(reference.GetObjectKey()),
	})
	if err != nil {
		return storedArtifact{}, fmt.Errorf("read reusable artifact %q: %w", reference.GetObjectKey(), err)
	}
	defer response.Body.Close()

	payload, err := io.ReadAll(io.LimitReader(response.Body, maxStoredArtifactBytes+1))
	if err != nil {
		return storedArtifact{}, fmt.Errorf("read reusable artifact body %q: %w", reference.GetObjectKey(), err)
	}
	if len(payload) > maxStoredArtifactBytes {
		return storedArtifact{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("artifact %q exceeds the aggregation limit", reference.GetObjectKey()),
			"InvalidReusableArtifact",
			nil,
		)
	}

	var artifact storedArtifact
	if err := json.Unmarshal(payload, &artifact); err != nil {
		return storedArtifact{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("artifact %q is not valid JSON", reference.GetObjectKey()),
			"InvalidReusableArtifact",
			err,
		)
	}
	return artifact, nil
}

func semanticArtifactContent(workflowID, activityID, activityVersion string) string {
	return fmt.Sprintf("Synthetic report section %s for workflow %s using activity version %s", activityID, workflowID, activityVersion)
}

func (a *ArtifactActivities) ensureBucket(ctx context.Context) error {
	response, err := a.client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return fmt.Errorf("list RustFS buckets: %w", err)
	}
	for _, bucket := range response.Buckets {
		if aws.ToString(bucket.Name) == a.bucket {
			return nil
		}
	}
	if _, err := a.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(a.bucket)}); err != nil {
		return fmt.Errorf("create RustFS bucket %q: %w", a.bucket, err)
	}
	return nil
}

func (a *ArtifactActivities) objectExists(ctx context.Context, objectKey string) (bool, error) {
	_, err := a.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(objectKey),
	})
	if err == nil {
		return true, nil
	}
	var responseError *smithyhttp.ResponseError
	if errors.As(err, &responseError) && responseError.HTTPStatusCode() == http.StatusNotFound {
		return false, nil
	}
	return false, err
}

func reusableArtifactKey(namespace, workflowID, activityID, activityType, activityVersion string) string {
	digest := sha256.New()
	for _, component := range []string{namespace, workflowID, activityID, activityType, activityVersion} {
		_, _ = digest.Write([]byte(component))
		_, _ = digest.Write([]byte{0})
	}
	return "reusable-activity-artifacts/" + hex.EncodeToString(digest.Sum(nil)) + ".json"
}

func performArtifactWork(ctx context.Context, duration time.Duration, attempt int32) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timer.C:
			return nil
		case <-ticker.C:
			activity.RecordHeartbeat(ctx, "performing-heavy-work", attempt)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
