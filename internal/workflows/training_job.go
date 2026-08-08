package workflows

import (
	"errors"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"

	orchestrationv1 "orchestration/gen/orchestration/v1"
	"orchestration/internal/workflowcatalog"
)

// TrainingJobWorkflow owns its shards. It waits for every one of them to settle before
// stating a resume point, because a resume point is only true at a step every shard has
// durably reached: resuming above it would advance one shard past work another never
// recorded. A parent that abandoned its children could never make that statement.
func TrainingJobWorkflow(ctx workflow.Context, input *orchestrationv1.TrainingJobRequest) (*orchestrationv1.TrainingJobResult, error) {
	if err := workflowcatalog.ValidateTrainingJobRequest(input); err != nil {
		return nil, invalidRequest("INVALID_TRAINING_JOB_REQUEST", err.Error())
	}

	status, err := newStatusTracker(ctx, int64(input.ShardCount), "training", "shards", "Training shards")
	if err != nil {
		return nil, err
	}

	// Watching cancellation in its own coroutine lets the job report that it is shutting
	// down, and withdraw the cancel action, while it is still waiting on its shards.
	var canceledAt time.Time
	workflow.Go(ctx, func(watchCtx workflow.Context) {
		watchCtx.Done().Receive(watchCtx, nil)
		canceledAt = workflow.Now(watchCtx)
		status.setShuttingDown("draining", "shards", "Cancelling shards and waiting for their checkpoints")
	})

	status.scheduleWork(int64(input.ShardCount))
	shards := scheduleTrainingShards(ctx, input)

	result := &orchestrationv1.TrainingJobResult{
		ShardCount: input.ShardCount,
		TotalSteps: input.TotalSteps,
		Shards:     make([]*orchestrationv1.ShardOutcome, len(shards)),
	}
	collectTrainingShards(ctx, shards, result, status)
	summarizeTrainingJob(result)
	if !canceledAt.IsZero() {
		result.ShutdownLatency = durationpb.New(workflow.Now(ctx).Sub(canceledAt))
	}

	if ctx.Err() != nil {
		status.setCanceled("canceled", "Shards stopped at a consistent checkpoint")
		return nil, temporal.NewCanceledError(trainingJobCanceled(result), result)
	}
	if !result.Complete {
		status.setFailed("failed", "One or more shards did not finish")
		return nil, workflowFailure(&orchestrationv1.WorkflowFailure{
			Code:      "TRAINING_SHARD_FAILED",
			Message:   "One or more training shards did not reach the requested step count.",
			Category:  orchestrationv1.FailureCategory_FAILURE_CATEGORY_DEPENDENCY,
			Retryable: true,
			Metadata:  map[string]string{"resumeStep": itoa(result.ResumeStep)},
		}, result)
	}

	status.setSucceeded("completed", "Training completed")
	return result, nil
}

func scheduleTrainingShards(ctx workflow.Context, input *orchestrationv1.TrainingJobRequest) []scheduledActivity {
	shards := make([]scheduledActivity, input.ShardCount)
	for index := range shards {
		shardID := workflowcatalog.ShardID(index)
		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID: workflowcatalog.TrainingShardWorkflowID(input.ExperimentId, index),
			// The parent does not finish until its cancelled shards have, which is what
			// makes its resume point a statement about the whole tree rather than a guess.
			WaitForCancellation: true,
			// If the parent closes for any other reason, shards are asked to stop rather
			// than killed, so they still commit the checkpoint they are holding.
			ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		})
		shards[index] = scheduledActivity{
			index:      index,
			activityID: shardID,
			future: workflow.ExecuteChildWorkflow(childCtx, workflowcatalog.TrainingShardWorkflowName, &orchestrationv1.TrainingShardRequest{
				ExperimentId:       input.ExperimentId,
				ShardId:            shardID,
				TotalSteps:         input.TotalSteps,
				StepsPerCheckpoint: input.StepsPerCheckpoint,
				StepDuration:       input.StepDuration,
				ShutdownGrace:      input.ShutdownGrace,
				FailureCase:        input.FailureCase,
			}),
		}
	}
	return shards
}

func collectTrainingShards(
	ctx workflow.Context,
	shards []scheduledActivity,
	result *orchestrationv1.TrainingJobResult,
	status *statusTracker,
) {
	// Shard outcomes are read on a context cancellation cannot touch: a cancelled shard
	// still reports where it stopped, and that report is what the resume point is built from.
	readCtx, releaseRead := workflow.NewDisconnectedContext(ctx)
	defer releaseRead()

	selector := workflow.NewSelector(ctx)
	remaining := len(shards)
	for _, shard := range shards {
		shard := shard
		selector.AddFuture(shard.future, func(future workflow.Future) {
			outcome := &orchestrationv1.ShardOutcome{}
			switch err := future.Get(readCtx, outcome); {
			case err == nil:
				status.recordSucceeded()
			case temporal.IsCanceledError(err):
				readShardOutcome(ctx, err, outcome)
				status.recordCanceled()
			default:
				workflow.GetLogger(ctx).Error("training shard failed", "shardId", shard.activityID, "error", err)
				readShardOutcome(ctx, err, outcome)
				outcome.State = orchestrationv1.ShardState_SHARD_STATE_FAILED
				status.recordFailed()
			}
			outcome.ShardId = shard.activityID
			result.Shards[shard.index] = outcome
			remaining--
			if ctx.Err() != nil {
				status.setShuttingDown("draining", shard.activityID, "Cancelling shards and waiting for their checkpoints")
				return
			}
			status.setRunning("training", shard.activityID, "Collecting shard outcomes")
		})
	}
	for remaining > 0 {
		selector.Select(ctx)
	}
}

// readShardOutcome recovers the outcome a shard attached to its terminal error, whether it
// was cancelled or failed. When it cannot be read the shard keeps checkpoint step zero,
// which understates the resume point rather than claiming progress no shard can prove.
func readShardOutcome(ctx workflow.Context, err error, outcome *orchestrationv1.ShardOutcome) {
	var canceled *temporal.CanceledError
	if errors.As(err, &canceled) {
		if canceled.HasDetails() && canceled.Details(outcome) == nil {
			return
		}
		workflow.GetLogger(ctx).Warn("shard cancellation outcome could not be read", "error", err)
		outcome.State = orchestrationv1.ShardState_SHARD_STATE_ABANDONED
		return
	}

	var failed *temporal.ApplicationError
	if errors.As(err, &failed) && failed.Type() == shardFailureErrorType && failed.HasDetails() {
		if decodeErr := failed.Details(outcome); decodeErr != nil {
			workflow.GetLogger(ctx).Warn("shard failure outcome could not be decoded", "error", decodeErr)
		}
	}
}

// summarizeTrainingJob derives the job's resume point from the shard that got least far.
func summarizeTrainingJob(result *orchestrationv1.TrainingJobResult) {
	resumeStep := int32(-1)
	complete := true
	resumedFrom := int32(-1)
	for _, shard := range result.Shards {
		if shard == nil {
			complete = false
			resumeStep = 0
			continue
		}
		if resumeStep < 0 || shard.CheckpointStep < resumeStep {
			resumeStep = shard.CheckpointStep
		}
		if resumedFrom < 0 || shard.ResumedFromStep < resumedFrom {
			resumedFrom = shard.ResumedFromStep
		}
		if shard.State != orchestrationv1.ShardState_SHARD_STATE_COMPLETED {
			complete = false
		}
	}
	result.ResumeStep = max(resumeStep, 0)
	result.ResumedFromStep = max(resumedFrom, 0)
	result.Complete = complete
}

func trainingJobCanceled(result *orchestrationv1.TrainingJobResult) *orchestrationv1.WorkflowFailure {
	return &orchestrationv1.WorkflowFailure{
		Code:     "TRAINING_JOB_CANCELED",
		Message:  "Training was cancelled; every shard stopped at a checkpoint and the job can resume.",
		Category: orchestrationv1.FailureCategory_FAILURE_CATEGORY_CANCELED,
		// Starting the same experiment again resumes from the recorded step.
		Retryable: true,
		Metadata:  map[string]string{"resumeStep": itoa(result.ResumeStep)},
	}
}
