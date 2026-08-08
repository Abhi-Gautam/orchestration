package workflows

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	orchestrationv1 "orchestration/gen/orchestration/v1"
	"orchestration/internal/activities"
)

const (
	checkpointIOTimeout  = 20 * time.Second
	trainIntervalMargin  = 30 * time.Second
	checkpointMaxAttempt = 3

	shardFailureErrorType = "TrainingShardFailed"
)

// TrainingShardWorkflow trains one shard, checkpointing every interval.
//
// Two contexts carry the design. Step work runs on a context cancellation does not reach,
// so the shard decides when to stop rather than having the decision made for it by the
// transport; checkpoint reads and writes run on another, so the durability boundary cannot
// be interrupted halfway by the very cancellation that makes a resume point necessary.
func TrainingShardWorkflow(ctx workflow.Context, input *orchestrationv1.TrainingShardRequest) (*orchestrationv1.ShardOutcome, error) {
	outcome := &orchestrationv1.ShardOutcome{ShardId: input.ShardId}

	durableCtx, releaseDurable := workflow.NewDisconnectedContext(ctx)
	defer releaseDurable()

	loaded, err := loadShardCheckpoint(durableCtx, input)
	if err != nil {
		return outcome, shardFailed(outcome, err)
	}
	outcome.ResumedFromStep = loaded.Step
	outcome.CompletedStep = loaded.Step
	outcome.CheckpointStep = loaded.Step
	outcome.CheckpointKey = loaded.Key
	outcome.RejectedCheckpoints = loaded.Rejected

	stepCtx, abandonSteps := workflow.NewDisconnectedContext(ctx)
	defer abandonSteps()

	for outcome.CompletedStep < input.TotalSteps {
		steps := min(input.StepsPerCheckpoint, input.TotalSteps-outcome.CompletedStep)
		interval := workflow.ExecuteActivity(
			workflow.WithActivityOptions(stepCtx, workflow.ActivityOptions{
				ActivityID:          intervalActivityID(outcome.CompletedStep),
				StartToCloseTimeout: intervalDuration(input, steps) + trainIntervalMargin,
				HeartbeatTimeout:    duration(input.StepDuration) + trainIntervalMargin,
				RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
			}),
			activities.TrainIntervalActivityName,
			activities.TrainIntervalInput{
				ShardID:      input.ShardId,
				FromStep:     outcome.CompletedStep,
				Steps:        steps,
				StepDuration: duration(input.StepDuration),
			},
		)

		result, finished, err := awaitInterval(ctx, interval, duration(input.ShutdownGrace))
		if err != nil {
			return outcome, shardFailed(outcome, err)
		}
		if !finished {
			// The grace period expired. Dropping the interval costs only the steps since
			// the last checkpoint, which is the whole reason a checkpoint exists.
			abandonSteps()
			outcome.State = orchestrationv1.ShardState_SHARD_STATE_ABANDONED
			return outcome, shardCanceled(outcome)
		}

		outcome.CompletedStep = result.CompletedStep
		key, err := publishShardCheckpoint(durableCtx, input, outcome.CompletedStep)
		if err != nil {
			return outcome, shardFailed(outcome, err)
		}
		outcome.CheckpointStep = outcome.CompletedStep
		outcome.CheckpointKey = key

		if ctx.Err() != nil {
			// Cancelled, but the interval finished inside its grace period and its
			// checkpoint is committed, so no work since the last checkpoint was lost.
			outcome.State = orchestrationv1.ShardState_SHARD_STATE_DRAINED
			return outcome, shardCanceled(outcome)
		}
	}

	outcome.State = orchestrationv1.ShardState_SHARD_STATE_COMPLETED
	return outcome, nil
}

// shardCanceled carries the shard's outcome out on its cancellation. Temporal delivers a
// result or an error, never both, so a cancelled shard that returned a bare error would tell
// its parent nothing about where it stopped, and the resume point is built from exactly that.
func shardCanceled(outcome *orchestrationv1.ShardOutcome) error {
	return temporal.NewCanceledError(outcome)
}

// shardFailed carries the outcome out on a failure for the same reason. A shard that fails
// has usually still checkpointed real progress, and dropping that would report its
// checkpoint as step zero and drag the job's resume point down to the same place.
func shardFailed(outcome *orchestrationv1.ShardOutcome, cause error) error {
	outcome.State = orchestrationv1.ShardState_SHARD_STATE_FAILED
	return temporal.NewApplicationErrorWithOptions(
		"training shard failed",
		shardFailureErrorType,
		temporal.ApplicationErrorOptions{Cause: cause, Details: []any{outcome}},
	)
}

// awaitInterval waits for the current interval, and once cancellation arrives gives it the
// shutdown grace to finish. The grace period is what bounds cancellation latency, and an
// unbounded one is the reason operators reach for termination instead.
func awaitInterval(ctx workflow.Context, interval workflow.Future, grace time.Duration) (activities.TrainIntervalResult, bool, error) {
	// A settled Future is read on a context cancellation cannot touch, so collecting the
	// interval's result never depends on whether the shard is still cancellable.
	readCtx, releaseRead := workflow.NewDisconnectedContext(ctx)
	defer releaseRead()

	var (
		result   activities.TrainIntervalResult
		finished bool
		getErr   error
	)
	collect := func(future workflow.Future) {
		getErr = future.Get(readCtx, &result)
		finished = true
	}

	if ctx.Err() == nil {
		waiting := workflow.NewSelector(ctx)
		waiting.AddFuture(interval, collect)
		waiting.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) {})
		waiting.Select(ctx)
		if finished {
			return result, true, getErr
		}
	}

	if grace <= 0 {
		return result, false, nil
	}

	graceCtx, releaseGrace := workflow.NewDisconnectedContext(ctx)
	defer releaseGrace()
	draining := workflow.NewSelector(graceCtx)
	draining.AddFuture(interval, collect)
	draining.AddFuture(workflow.NewTimer(graceCtx, grace), func(workflow.Future) {})
	draining.Select(graceCtx)
	return result, finished, getErr
}

func loadShardCheckpoint(ctx workflow.Context, input *orchestrationv1.TrainingShardRequest) (activities.LoadCheckpointResult, error) {
	var loaded activities.LoadCheckpointResult
	err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, checkpointActivityOptions("load-checkpoint")),
		activities.LoadCheckpointActivityName,
		activities.LoadCheckpointInput{ExperimentID: input.ExperimentId, ShardID: input.ShardId},
	).Get(ctx, &loaded)
	return loaded, err
}

func publishShardCheckpoint(ctx workflow.Context, input *orchestrationv1.TrainingShardRequest, step int32) (string, error) {
	var key string
	err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, checkpointActivityOptions(checkpointActivityID(step))),
		activities.PublishCheckpointActivityName,
		activities.PublishCheckpointInput{
			ExperimentID: input.ExperimentId,
			ShardID:      input.ShardId,
			Step:         step,
			FailureCase:  input.FailureCase,
		},
	).Get(ctx, &key)
	return key, err
}

// checkpointActivityOptions bound the durable work so cancellation cannot hang on it. A
// wedged checkpoint leaves termination as the only escape, which is the case being compared.
func checkpointActivityOptions(activityID string) workflow.ActivityOptions {
	return workflow.ActivityOptions{
		ActivityID:          activityID,
		StartToCloseTimeout: checkpointIOTimeout,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: checkpointMaxAttempt},
	}
}

func intervalDuration(input *orchestrationv1.TrainingShardRequest, steps int32) time.Duration {
	return duration(input.StepDuration) * time.Duration(steps)
}

func intervalActivityID(fromStep int32) string {
	return fmt.Sprintf("interval-from-%08d", fromStep)
}

func checkpointActivityID(step int32) string {
	return fmt.Sprintf("checkpoint-%08d", step)
}
