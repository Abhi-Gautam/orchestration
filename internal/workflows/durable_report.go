package workflows

import (
	"errors"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	orchestrationv1 "orchestration/gen/orchestration/v1"
	"orchestration/internal/activities"
	"orchestration/internal/workflowcatalog"
)

const (
	durableReportArtifactCount = 5
	durableReportActivityCount = durableReportArtifactCount + 2
)

func DurableReportWorkflow(ctx workflow.Context, input *orchestrationv1.DurableReportRequest) (*orchestrationv1.DurableReportResult, error) {
	heavyWorkDuration, err := workflowcatalog.ValidateDurableReportRequest(input)
	if err != nil {
		return nil, invalidRequest("INVALID_DURABLE_REPORT_REQUEST", err.Error())
	}

	status, err := newStatusTracker(
		ctx,
		durableReportActivityCount,
		"generating-artifacts",
		workflowcatalog.ArtifactActivityIDs(durableReportArtifactCount),
		"Generating reusable report artifacts",
	)
	if err != nil {
		return nil, err
	}

	producerOptions := workflow.ActivityOptions{
		StartToCloseTimeout: heavyWorkDuration + workflowcatalog.ArtifactActivityTimeoutMargin,
		HeartbeatTimeout:    5 * time.Second,
		WaitForCancellation: true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 1,
			MaximumAttempts:    2,
		},
	}
	status.scheduleWork(durableReportArtifactCount)
	producerExecutions := make([]scheduledActivity, durableReportArtifactCount)
	for index := range producerExecutions {
		activityID := workflowcatalog.ArtifactActivityID(index)
		activityCtx := workflow.WithActivityOptions(ctx, withActivityID(producerOptions, activityID))
		producerExecutions[index] = scheduledActivity{
			index:      index,
			activityID: activityID,
			future: workflow.ExecuteActivity(activityCtx, activities.GenerateArtifactActivityName, activities.GenerateArtifactInput{
				ActivityVersion: input.ActivityVersion,
				WorkDuration:    heavyWorkDuration,
				FailureCase:     orchestrationv1.ReusableArtifactFailureCase_REUSABLE_ARTIFACT_FAILURE_CASE_NONE,
			}),
		}
	}

	references := make([]*orchestrationv1.ArtifactReference, durableReportArtifactCount)
	selector := workflow.NewSelector(ctx)
	remaining := durableReportArtifactCount
	var producerErr error
	for _, execution := range producerExecutions {
		execution := execution
		selector.AddFuture(execution.future, func(future workflow.Future) {
			var reference orchestrationv1.ArtifactReference
			if futureErr := future.Get(ctx, &reference); futureErr != nil {
				status.recordFailed()
				if producerErr == nil {
					producerErr = futureErr
				}
			} else {
				references[execution.index] = &reference
				status.recordSucceeded()
			}
			remaining--
			status.setRunning("generating-artifacts", execution.activityID, "Collecting reusable report artifact references")
		})
	}
	for remaining > 0 {
		selector.Select(ctx)
	}

	if ctx.Err() != nil {
		status.setCanceled("canceled", "Durable report generation was canceled")
		return nil, ctx.Err()
	}
	if producerErr != nil {
		status.setFailed("failed", "Report artifact generation failed")
		return nil, reportDependencyFailure("REPORT_ARTIFACT_GENERATION_FAILED", "Report artifact generation failed after retries.", "generating-artifacts", producerErr)
	}

	status.scheduleWork(1)
	status.setRunning("aggregating", "aggregate-artifacts", "Consuming all report artifacts")
	aggregateCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:          "aggregate-artifacts",
		StartToCloseTimeout: 30 * time.Second,
		WaitForCancellation: true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 1,
			MaximumAttempts:    2,
		},
	})
	var summary activities.ReportSummary
	err = workflow.ExecuteActivity(aggregateCtx, activities.AggregateArtifactsActivityName, activities.AggregateArtifactsInput{
		References:      references,
		ActivityVersion: input.ActivityVersion,
		InjectFailure:   input.FailureCase == orchestrationv1.DurableReportFailureCase_DURABLE_REPORT_FAILURE_CASE_AGGREGATION_RETRYABLE,
	}).Get(ctx, &summary)
	if err != nil {
		if ctx.Err() != nil {
			status.recordCanceled()
			status.setCanceled("canceled", "Durable report aggregation was canceled")
			return nil, ctx.Err()
		}
		status.recordFailed()
		status.setFailed("failed", "Report artifact aggregation failed")
		return nil, reportDependencyFailure("ARTIFACT_AGGREGATION_FAILED", "Report artifact aggregation failed after retries.", "aggregating", err)
	}

	status.recordSucceeded()
	status.scheduleWork(1)
	status.setRunning("persisting", "persist-report", "Persisting the durable report")
	persistCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:          "persist-report",
		StartToCloseTimeout: 10 * time.Second,
		WaitForCancellation: true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 1,
			MaximumAttempts:    2,
		},
	})
	var record activities.ReportRecord
	err = workflow.ExecuteActivity(persistCtx, activities.PersistReportActivityName, activities.PersistReportInput{
		ReportID:    input.ReportId,
		Summary:     summary,
		FailureCase: input.FailureCase,
	}).Get(ctx, &record)
	if err != nil {
		if ctx.Err() != nil {
			status.recordCanceled()
			status.setCanceled("canceled", "Durable report persistence was canceled")
			return nil, ctx.Err()
		}
		status.recordFailed()
		status.setFailed("failed", "Durable report persistence failed")
		var applicationErr *temporal.ApplicationError
		if errors.As(err, &applicationErr) && applicationErr.Type() == activities.ReportIdempotencyConflictErrorType {
			return nil, reportConflictFailure(applicationErr.Message(), "persisting", err)
		}
		return nil, reportDependencyFailure("REPORT_PERSISTENCE_FAILED", "Durable report persistence failed after retries.", "persisting", err)
	}

	result := &orchestrationv1.DurableReportResult{
		ReportId:       record.ReportID,
		ArtifactCount:  record.ArtifactCount,
		SemanticDigest: record.SemanticDigest,
	}
	status.recordSucceeded()
	status.setSucceeded("completed", "Durable report is ready")
	return result, nil
}

// reportDependencyFailure marks work that could succeed if the operation is started again.
func reportDependencyFailure(code, message, stage string, err error) error {
	return workflowFailure(&orchestrationv1.WorkflowFailure{
		Code:      code,
		Message:   message,
		Category:  orchestrationv1.FailureCategory_FAILURE_CATEGORY_DEPENDENCY,
		Retryable: true,
		Metadata:  causeMetadata(stage, err),
	}, nil)
}

// reportConflictFailure marks a business decision. The report ID already holds different
// content, so starting the same operation again produces the same conflict.
func reportConflictFailure(message, stage string, err error) error {
	return workflowFailure(&orchestrationv1.WorkflowFailure{
		Code:      "REPORT_IDEMPOTENCY_CONFLICT",
		Message:   message,
		Category:  orchestrationv1.FailureCategory_FAILURE_CATEGORY_BUSINESS,
		Retryable: false,
		Metadata:  causeMetadata(stage, err),
	}, nil)
}
