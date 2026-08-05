package workflows

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	orchestrationv1 "orchestration/gen/orchestration/v1"
	"orchestration/internal/activities"
	"orchestration/internal/workflowcatalog"
)

const reusableArtifactCount = workflowcatalog.ReusableArtifactCount

type reusableArtifactExecution struct {
	index      int
	activityID string
	future     workflow.Future
}

func ReusableArtifactWorkflow(ctx workflow.Context, input *orchestrationv1.ReusableArtifactRequest) (*orchestrationv1.ReusableArtifactResult, error) {
	heavyWorkDuration, err := workflowcatalog.ValidateReusableArtifactRequest(input)
	if err != nil {
		return nil, invalidRequest("INVALID_REUSABLE_ARTIFACT_REQUEST", err.Error())
	}

	status, err := newStatusTracker(
		ctx,
		"generating",
		workflowcatalog.ArtifactActivityIDs(reusableArtifactCount),
		"Generating reusable artifacts",
		operationProgress(reusableArtifactCount, reusableArtifactCount, reusableArtifactCount, 0, 0, 0),
	)
	if err != nil {
		return nil, err
	}

	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: heavyWorkDuration + workflowcatalog.ArtifactActivityTimeoutMargin,
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
		activityID := workflowcatalog.ArtifactActivityID(index)
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
