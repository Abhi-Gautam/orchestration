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

type durableReportArtifactExecution struct {
	index      int
	activityID string
	future     workflow.Future
}

func DurableReportWorkflow(ctx workflow.Context, input *orchestrationv1.DurableReportRequest) (*orchestrationv1.DurableReportResult, error) {
	heavyWorkDuration, err := workflowcatalog.ValidateDurableReportRequest(input)
	if err != nil {
		return nil, invalidRequest("INVALID_DURABLE_REPORT_REQUEST", err.Error())
	}

	status, err := newStatusTracker(
		ctx,
		"generating-artifacts",
		workflowcatalog.ArtifactActivityIDs(durableReportArtifactCount),
		"Generating reusable report artifacts",
		operationProgress(durableReportActivityCount, durableReportArtifactCount, durableReportArtifactCount, 0, 0, 0),
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
	producerExecutions := make([]durableReportArtifactExecution, durableReportArtifactCount)
	for index := range producerExecutions {
		activityID := workflowcatalog.ArtifactActivityID(index)
		activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:          activityID,
			StartToCloseTimeout: producerOptions.StartToCloseTimeout,
			HeartbeatTimeout:    producerOptions.HeartbeatTimeout,
			WaitForCancellation: producerOptions.WaitForCancellation,
			RetryPolicy:         producerOptions.RetryPolicy,
		})
		producerExecutions[index] = durableReportArtifactExecution{
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
	succeeded := int64(0)
	failed := int64(0)
	var producerErr error
	for _, execution := range producerExecutions {
		execution := execution
		selector.AddFuture(execution.future, func(future workflow.Future) {
			var reference orchestrationv1.ArtifactReference
			if futureErr := future.Get(ctx, &reference); futureErr != nil {
				failed++
				if producerErr == nil {
					producerErr = futureErr
				}
			} else {
				references[execution.index] = &reference
				succeeded++
			}
			remaining--
			status.setRunning(
				"generating-artifacts",
				execution.activityID,
				"Collecting reusable report artifact references",
				operationProgress(durableReportActivityCount, durableReportArtifactCount, int64(remaining), succeeded, failed, 0),
			)
		})
	}
	for remaining > 0 {
		selector.Select(ctx)
	}

	if ctx.Err() != nil {
		status.setCanceled("canceled", "Durable report generation was canceled", operationProgress(durableReportActivityCount, durableReportArtifactCount, 0, succeeded, failed, 0))
		return nil, ctx.Err()
	}
	if producerErr != nil {
		status.setFailed("failed", "Report artifact generation failed", operationProgress(durableReportActivityCount, durableReportArtifactCount, 0, succeeded, failed, 0))
		return nil, durableReportFailure("REPORT_ARTIFACT_GENERATION_FAILED", "Report artifact generation failed after retries.", "generating-artifacts", producerErr)
	}

	status.setRunning(
		"aggregating",
		"aggregate-artifacts",
		"Consuming all report artifacts",
		operationProgress(durableReportActivityCount, durableReportArtifactCount+1, 1, durableReportArtifactCount, 0, 0),
	)
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
			status.setCanceled("canceled", "Durable report aggregation was canceled", operationProgress(durableReportActivityCount, durableReportArtifactCount+1, 0, durableReportArtifactCount, 0, 1))
			return nil, ctx.Err()
		}
		status.setFailed("failed", "Report artifact aggregation failed", operationProgress(durableReportActivityCount, durableReportArtifactCount+1, 0, durableReportArtifactCount, 1, 0))
		return nil, durableReportFailure("ARTIFACT_AGGREGATION_FAILED", "Report artifact aggregation failed after retries.", "aggregating", err)
	}

	status.setRunning(
		"persisting",
		"persist-report",
		"Persisting the durable report",
		operationProgress(durableReportActivityCount, durableReportActivityCount, 1, durableReportArtifactCount+1, 0, 0),
	)
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
			status.setCanceled("canceled", "Durable report persistence was canceled", operationProgress(durableReportActivityCount, durableReportActivityCount, 0, durableReportArtifactCount+1, 0, 1))
			return nil, ctx.Err()
		}
		status.setFailed("failed", "Durable report persistence failed", operationProgress(durableReportActivityCount, durableReportActivityCount, 0, durableReportArtifactCount+1, 1, 0))
		var applicationErr *temporal.ApplicationError
		if errors.As(err, &applicationErr) && applicationErr.Type() == "ReportIdempotencyConflict" {
			return nil, durableReportFailure("REPORT_IDEMPOTENCY_CONFLICT", applicationErr.Message(), "persisting", err)
		}
		return nil, durableReportFailure("REPORT_PERSISTENCE_FAILED", "Durable report persistence failed after retries.", "persisting", err)
	}

	result := &orchestrationv1.DurableReportResult{
		ReportId:       record.ReportID,
		ArtifactCount:  record.ArtifactCount,
		SemanticDigest: record.SemanticDigest,
	}
	status.setSucceeded("completed", "Durable report is ready", operationProgress(durableReportActivityCount, durableReportActivityCount, 0, durableReportActivityCount, 0, 0))
	return result, nil
}

func durableReportFailure(code, message, stage string, err error) error {
	failure := &orchestrationv1.WorkflowFailure{
		Code:      code,
		Message:   message,
		Category:  orchestrationv1.FailureCategory_FAILURE_CATEGORY_DEPENDENCY,
		Retryable: true,
		Metadata:  map[string]string{"stage": stage},
	}
	if code == "REPORT_IDEMPOTENCY_CONFLICT" {
		failure.Category = orchestrationv1.FailureCategory_FAILURE_CATEGORY_BUSINESS
		failure.Retryable = false
	}
	var applicationErr *temporal.ApplicationError
	if errors.As(err, &applicationErr) {
		failure.Metadata["temporalType"] = applicationErr.Type()
	}
	return temporal.NewNonRetryableApplicationError(message, "DurableReportFailure", nil, failure)
}
