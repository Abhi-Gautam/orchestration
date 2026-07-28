package workflows

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	orchestrationv1 "orchestration/gen/orchestration/v1"
	"orchestration/internal/activities"
)

const (
	ReusableArtifactWorkflowName = "ReusableArtifactWorkflow"
	reusableArtifactCount        = 5
)

type reusableArtifactExecution struct {
	index      int
	activityID string
	future     workflow.Future
}

func ReusableArtifactWorkflow(ctx workflow.Context, input *orchestrationv1.ReusableArtifactRequest) (*orchestrationv1.ReusableArtifactResult, error) {
	heavyWorkDuration, err := validateReusableArtifactRequest(input)
	if err != nil {
		return nil, invalidRequest("INVALID_REUSABLE_ARTIFACT_REQUEST", err.Error())
	}

	status, err := newStatusTracker(
		ctx,
		"generating",
		"artifact-000,artifact-001,artifact-002,artifact-003,artifact-004",
		"Generating reusable artifacts",
		operationProgress(reusableArtifactCount, reusableArtifactCount, reusableArtifactCount, 0, 0, 0),
	)
	if err != nil {
		return nil, err
	}

	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: heavyWorkDuration + 30*time.Second,
		HeartbeatTimeout:    5 * time.Second,
		WaitForCancellation: true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 1,
			MaximumAttempts:    2,
		},
	}
	activityCtx := workflow.WithActivityOptions(ctx, activityOptions)
	executions := make([]reusableArtifactExecution, reusableArtifactCount)
	for index := range executions {
		activityID := fmt.Sprintf("artifact-%03d", index)
		failureCase := orchestrationv1.ReusableArtifactFailureCase_REUSABLE_ARTIFACT_FAILURE_CASE_NONE
		if activityID == input.FailureTargetActivity {
			failureCase = input.FailureCase
		}
		executionCtx := workflow.WithActivityOptions(activityCtx, workflow.ActivityOptions{
			ActivityID:          activityID,
			StartToCloseTimeout: activityOptions.StartToCloseTimeout,
			HeartbeatTimeout:    activityOptions.HeartbeatTimeout,
			WaitForCancellation: activityOptions.WaitForCancellation,
			RetryPolicy:         activityOptions.RetryPolicy,
		})
		executions[index] = reusableArtifactExecution{
			index:      index,
			activityID: activityID,
			future: workflow.ExecuteActivity(executionCtx, activities.GenerateArtifactActivityName, activities.GenerateArtifactInput{
				ActivityVersion: input.ActivityVersion,
				WorkDuration:    heavyWorkDuration,
				FailureCase:     failureCase,
			}),
		}
	}

	result := &orchestrationv1.ReusableArtifactResult{
		Artifacts: make([]*orchestrationv1.ArtifactReference, reusableArtifactCount),
	}
	selector := workflow.NewSelector(ctx)
	remaining := reusableArtifactCount
	succeeded := int64(0)
	failed := int64(0)
	var firstErr error
	for _, execution := range executions {
		execution := execution
		selector.AddFuture(execution.future, func(future workflow.Future) {
			var reference orchestrationv1.ArtifactReference
			if futureErr := future.Get(ctx, &reference); futureErr != nil {
				failed++
				if firstErr == nil {
					firstErr = futureErr
				}
			} else {
				result.Artifacts[execution.index] = &reference
				succeeded++
			}
			remaining--
			status.setRunning(
				"generating",
				execution.activityID,
				"Collecting reusable artifact references",
				operationProgress(reusableArtifactCount, reusableArtifactCount, int64(remaining), succeeded, failed, 0),
			)
		})
	}

	for remaining > 0 {
		selector.Select(ctx)
	}
	progress := operationProgress(reusableArtifactCount, reusableArtifactCount, 0, succeeded, failed, 0)
	if ctx.Err() != nil {
		status.setCanceled("canceled", "Reusable artifact generation was canceled", progress)
		return result, ctx.Err()
	}
	if firstErr != nil {
		status.setFailed("failed", "Reusable artifact generation failed", progress)
		return result, firstErr
	}

	status.setSucceeded("completed", "Reusable artifacts are ready", progress)
	return result, nil
}

func ReusableArtifactWorkflowID(input *orchestrationv1.ReusableArtifactRequest) (string, error) {
	if _, err := validateReusableArtifactRequest(input); err != nil {
		return "", err
	}
	return "reusable-artifacts/" + input.ExperimentId, nil
}

func validateReusableArtifactRequest(input *orchestrationv1.ReusableArtifactRequest) (time.Duration, error) {
	if input == nil {
		return 0, errors.New("input is required")
	}
	if !validExperimentID(input.ExperimentId) {
		return 0, errors.New("experiment ID must be 1-128 characters using letters, numbers, dot, underscore, or hyphen")
	}
	if input.ActivityVersion == "" || input.ActivityVersion != strings.TrimSpace(input.ActivityVersion) {
		return 0, errors.New("activity version is required and must not have surrounding whitespace")
	}
	if input.HeavyWorkDuration == nil || input.HeavyWorkDuration.CheckValid() != nil {
		return 0, errors.New("heavy-work duration must be a valid protobuf duration")
	}
	heavyWorkDuration := input.HeavyWorkDuration.AsDuration()
	if heavyWorkDuration <= 0 {
		return 0, errors.New("heavy-work duration must be positive")
	}
	switch input.FailureCase {
	case orchestrationv1.ReusableArtifactFailureCase_REUSABLE_ARTIFACT_FAILURE_CASE_NONE:
	case orchestrationv1.ReusableArtifactFailureCase_REUSABLE_ARTIFACT_FAILURE_CASE_BEFORE_PUBLICATION,
		orchestrationv1.ReusableArtifactFailureCase_REUSABLE_ARTIFACT_FAILURE_CASE_AFTER_PUBLICATION:
		if !validReusableArtifactID(input.FailureTargetActivity) {
			return 0, errors.New("failure target must be one of artifact-000 through artifact-004")
		}
	default:
		return 0, errors.New("failure case must be none, before publication, or after publication")
	}
	return heavyWorkDuration, nil
}

func validExperimentID(value string) bool {
	if len(value) == 0 || len(value) > 128 || value != strings.TrimSpace(value) {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '.' || character == '_' || character == '-' {
			continue
		}
		return false
	}
	return true
}

func validReusableArtifactID(value string) bool {
	for index := 0; index < reusableArtifactCount; index++ {
		if value == fmt.Sprintf("artifact-%03d", index) {
			return true
		}
	}
	return false
}
