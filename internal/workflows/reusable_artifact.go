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
		reusableArtifactCount,
		"generating",
		workflowcatalog.ArtifactActivityIDs(reusableArtifactCount),
		"Generating reusable artifacts",
	)
	if err != nil {
		return nil, err
	}
	status.scheduleWork(reusableArtifactCount)

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
	var firstErr error
	for _, execution := range executions {
		execution := execution
		selector.AddFuture(execution.future, func(future workflow.Future) {
			var reference orchestrationv1.ArtifactReference
			if futureErr := future.Get(ctx, &reference); futureErr != nil {
				status.recordFailed()
				if firstErr == nil {
					firstErr = futureErr
				}
			} else {
				result.Artifacts[execution.index] = &reference
				status.recordSucceeded()
			}
			remaining--
			status.setRunning("generating", execution.activityID, "Collecting reusable artifact references")
		})
	}

	for remaining > 0 {
		selector.Select(ctx)
	}
	if ctx.Err() != nil {
		status.setCanceled("canceled", "Reusable artifact generation was canceled")
		return result, ctx.Err()
	}
	if firstErr != nil {
		status.setFailed("failed", "Reusable artifact generation failed")
		return result, firstErr
	}

	status.setSucceeded("completed", "Reusable artifacts are ready")
	return result, nil
}
