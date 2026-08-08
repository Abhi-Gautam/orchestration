package activities

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	orchestrationv1 "orchestration/gen/orchestration/v1"
)

const (
	TrainIntervalActivityName     = "TrainIntervalActivity"
	PublishCheckpointActivityName = "PublishCheckpointActivity"
	LoadCheckpointActivityName    = "LoadCheckpointActivity"

	maxCheckpointBytes = 1 << 20
)

type TrainIntervalInput struct {
	ShardID      string
	FromStep     int32
	Steps        int32
	StepDuration time.Duration
}

type TrainIntervalResult struct {
	CompletedStep int32
}

type PublishCheckpointInput struct {
	ExperimentID string
	ShardID      string
	Step         int32
	FailureCase  orchestrationv1.CheckpointFailureCase
}

type LoadCheckpointInput struct {
	ExperimentID string
	ShardID      string
}

type LoadCheckpointResult struct {
	Step     int32
	Key      string
	Rejected int32
}

// storedCheckpoint carries its own checksum so a reader can tell a complete checkpoint from
// one whose write was interrupted. Trusting the object's presence is the mistake this
// experiment exists to show.
type storedCheckpoint struct {
	ExperimentID string    `json:"experimentId"`
	ShardID      string    `json:"shardId"`
	Step         int32     `json:"step"`
	State        string    `json:"state"`
	Checksum     string    `json:"checksum"`
	PublishedAt  time.Time `json:"publishedAt"`
}

// TrainIntervalActivity is the shard's atomic unit of work. Cancelling a shard mid-interval
// loses only the steps since its last checkpoint, which is what makes cancelling cheap
// enough to prefer over termination.
func TrainIntervalActivity(ctx context.Context, input TrainIntervalInput) (TrainIntervalResult, error) {
	if input.Steps <= 0 || input.StepDuration <= 0 {
		return TrainIntervalResult{}, temporal.NewNonRetryableApplicationError(
			"interval requires a positive step count and duration",
			"InvalidTrainInterval",
			nil,
			Attempt(ctx),
		)
	}

	logger := activity.GetLogger(ctx)
	step := input.FromStep
	for completed := int32(0); completed < input.Steps; completed++ {
		select {
		case <-time.After(input.StepDuration):
			step++
			activity.RecordHeartbeat(ctx, Attempt(ctx))
		case <-ctx.Done():
			// The Workflow abandoned this interval. Its steps are not checkpointed, so the
			// shard resumes from its last checkpoint rather than from here.
			logger.Info("TrainIntervalActivity abandoned", "shardId", input.ShardID, "step", step)
			return TrainIntervalResult{}, temporal.NewCanceledError(Attempt(ctx))
		}
	}
	return TrainIntervalResult{CompletedStep: step}, nil
}

func (a *ArtifactActivities) PublishCheckpointActivity(ctx context.Context, input PublishCheckpointInput) (string, error) {
	key := checkpointKey(input.ExperimentID, input.ShardID, input.Step)
	state := fmt.Sprintf("weights for %s at step %d", input.ShardID, input.Step)
	checkpoint := storedCheckpoint{
		ExperimentID: input.ExperimentID,
		ShardID:      input.ShardID,
		Step:         input.Step,
		State:        state,
		Checksum:     checksum(state),
		PublishedAt:  time.Now().UTC(),
	}
	if input.FailureCase == orchestrationv1.CheckpointFailureCase_CHECKPOINT_FAILURE_CASE_TORN_WRITE {
		// Stand in for a write that stopped partway: the object exists and parses, but its
		// contents do not match the checksum it claims.
		checkpoint.State = state[:len(state)/2]
	}

	payload, err := json.Marshal(checkpoint)
	if err != nil {
		return "", fmt.Errorf("encode checkpoint: %w", err)
	}
	if _, err := a.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(a.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(payload),
		ContentType: aws.String("application/json"),
	}); err != nil {
		return "", fmt.Errorf("publish checkpoint %q: %w", key, err)
	}

	activity.GetLogger(ctx).Info("PublishCheckpointActivity published",
		"shardId", input.ShardID, "step", input.Step, "key", key, "torn",
		input.FailureCase == orchestrationv1.CheckpointFailureCase_CHECKPOINT_FAILURE_CASE_TORN_WRITE)
	return key, nil
}

// LoadCheckpointActivity returns the newest checkpoint a shard can actually resume from. It
// walks backwards, and deletes any checkpoint whose contents do not match its checksum, so a
// torn write neither resumes the shard from a lie nor blocks it forever.
func (a *ArtifactActivities) LoadCheckpointActivity(ctx context.Context, input LoadCheckpointInput) (LoadCheckpointResult, error) {
	prefix := checkpointPrefix(input.ExperimentID, input.ShardID)
	listed, err := a.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(a.bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return LoadCheckpointResult{}, fmt.Errorf("list checkpoints for %q: %w", prefix, err)
	}

	keys := make([]string, 0, len(listed.Contents))
	for _, object := range listed.Contents {
		keys = append(keys, aws.ToString(object.Key))
	}
	sort.Sort(sort.Reverse(sort.StringSlice(keys)))

	result := LoadCheckpointResult{}
	logger := activity.GetLogger(ctx)
	for _, key := range keys {
		checkpoint, err := a.readCheckpoint(ctx, key)
		if err == nil && checkpoint.Checksum == checksum(checkpoint.State) && checkpoint.ShardID == input.ShardID {
			result.Step = checkpoint.Step
			result.Key = key
			return result, nil
		}
		logger.Warn("LoadCheckpointActivity rejected a checkpoint", "key", key, "error", err)
		result.Rejected++
		if _, deleteErr := a.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(a.bucket),
			Key:    aws.String(key),
		}); deleteErr != nil {
			return LoadCheckpointResult{}, fmt.Errorf("discard invalid checkpoint %q: %w", key, deleteErr)
		}
	}
	return result, nil
}

func (a *ArtifactActivities) readCheckpoint(ctx context.Context, key string) (storedCheckpoint, error) {
	response, err := a.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return storedCheckpoint{}, err
	}
	defer response.Body.Close()

	payload, err := io.ReadAll(io.LimitReader(response.Body, maxCheckpointBytes+1))
	if err != nil {
		return storedCheckpoint{}, err
	}
	if len(payload) > maxCheckpointBytes {
		return storedCheckpoint{}, fmt.Errorf("checkpoint %q exceeds the size limit", key)
	}
	var checkpoint storedCheckpoint
	if err := json.Unmarshal(payload, &checkpoint); err != nil {
		return storedCheckpoint{}, err
	}
	return checkpoint, nil
}

// checkpointKey pads the step so lexical order matches numeric order, which lets a prefix
// listing find the newest checkpoint without reading every object.
func checkpointKey(experimentID, shardID string, step int32) string {
	return fmt.Sprintf("%sstep-%08d.json", checkpointPrefix(experimentID, shardID), step)
}

func checkpointPrefix(experimentID, shardID string) string {
	return fmt.Sprintf("training-job/%s/%s/", strings.TrimSpace(experimentID), strings.TrimSpace(shardID))
}

func checksum(state string) string {
	sum := sha256.Sum256([]byte(state))
	return hex.EncodeToString(sum[:])
}
