package workflows

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"orchestration/internal/activities"
)

const FanOutPolicyWorkflowName = "FanOutPolicyWorkflow"

type AggregationPolicy string

const (
	FailFast           AggregationPolicy = "fail-fast"
	AllSettled         AggregationPolicy = "all-settled"
	AllSettledThenFail AggregationPolicy = "all-settled-then-fail"
)

type ActivityFailureKind string

const (
	ApplicationFailure ActivityFailureKind = "application"
	TimeoutFailure     ActivityFailureKind = "timeout"
	CanceledFailure    ActivityFailureKind = "canceled"
	PanicFailure       ActivityFailureKind = "panic"
	UnknownFailure     ActivityFailureKind = "unknown"
)

type FaultBranch struct {
	ActivityID             string
	Input                  activities.FaultActivityInput
	StartToCloseTimeout    time.Duration
	ScheduleToCloseTimeout time.Duration
	HeartbeatTimeout       time.Duration
	RetryPolicy            temporal.RetryPolicy
}

type ActivityFailure struct {
	Kind         ActivityFailureKind
	Message      string
	Type         string
	NonRetryable bool
	TimeoutType  string
}

type ActivityOutcome struct {
	ActivityID string
	Index      int
	Result     *activities.FaultActivityResult
	Failure    *ActivityFailure
}

type FanOutResult struct {
	Policy     AggregationPolicy
	Planned    int
	Succeeded  int
	Failed     int
	Canceled   int
	Outcomes   []ActivityOutcome
	Finalize   *activities.WaitResult
	StartedAt  time.Time
	FinishedAt time.Time
	Elapsed    time.Duration
}

type FanOutPolicyInput struct {
	Policy           AggregationPolicy
	Branches         []FaultBranch
	FinalizeDuration time.Duration
}

type scheduledActivity struct {
	index      int
	activityID string
	future     workflow.Future
}

func FanOutPolicyWorkflow(ctx workflow.Context, input FanOutPolicyInput) (FanOutResult, error) {
	startedAt := workflow.Now(ctx)
	activityCtx, cancelActivities := workflow.WithCancel(ctx)
	defer cancelActivities()

	scheduled := scheduleFaultActivities(activityCtx, input.Branches)
	result, err := aggregateFaultActivities(ctx, scheduled, input.Policy, cancelActivities)
	result.Policy = input.Policy
	result.StartedAt = startedAt
	if err != nil {
		result.FinishedAt = workflow.Now(ctx)
		result.Elapsed = result.FinishedAt.Sub(startedAt)
		return result, err
	}

	if input.FinalizeDuration > 0 {
		var finalize activities.WaitResult
		if err := scheduleWaitActivity(ctx, "finalize", input.FinalizeDuration).Get(ctx, &finalize); err != nil {
			return result, err
		}
		result.Finalize = &finalize
	}

	result.FinishedAt = workflow.Now(ctx)
	result.Elapsed = result.FinishedAt.Sub(startedAt)
	return result, nil
}

func scheduleFaultActivities(ctx workflow.Context, branches []FaultBranch) []scheduledActivity {
	scheduled := make([]scheduledActivity, len(branches))
	for i, branch := range branches {
		options := workflow.ActivityOptions{
			ActivityID:             branch.ActivityID,
			StartToCloseTimeout:    branch.StartToCloseTimeout,
			ScheduleToCloseTimeout: branch.ScheduleToCloseTimeout,
			HeartbeatTimeout:       branch.HeartbeatTimeout,
			WaitForCancellation:    true,
			RetryPolicy:            &branch.RetryPolicy,
		}
		activityCtx := workflow.WithActivityOptions(ctx, options)
		scheduled[i] = scheduledActivity{
			index:      i,
			activityID: branch.ActivityID,
			future: workflow.ExecuteActivity(
				activityCtx,
				activities.FaultInjectionActivity,
				branch.Input,
			),
		}
	}
	return scheduled
}

func aggregateFaultActivities(
	ctx workflow.Context,
	scheduled []scheduledActivity,
	policy AggregationPolicy,
	cancelActivities workflow.CancelFunc,
) (FanOutResult, error) {
	result := FanOutResult{
		Planned:  len(scheduled),
		Outcomes: make([]ActivityOutcome, len(scheduled)),
	}
	selector := workflow.NewSelector(ctx)
	remaining := len(scheduled)
	var firstErr error

	for _, item := range scheduled {
		item := item
		selector.AddFuture(item.future, func(f workflow.Future) {
			outcome := ActivityOutcome{ActivityID: item.activityID, Index: item.index}
			var activityResult activities.FaultActivityResult
			if err := f.Get(ctx, &activityResult); err != nil {
				failure := classifyActivityFailure(err)
				outcome.Failure = &failure
				if failure.Kind == CanceledFailure {
					result.Canceled++
				} else {
					result.Failed++
				}
				if policy == FailFast && firstErr == nil {
					firstErr = err
					cancelActivities()
				}
			} else {
				outcome.Result = &activityResult
				result.Succeeded++
			}
			result.Outcomes[item.index] = outcome
			remaining--
		})
	}

	for remaining > 0 {
		selector.Select(ctx)
	}

	if firstErr != nil {
		return result, firstErr
	}
	if policy == AllSettledThenFail && (result.Failed > 0 || result.Canceled > 0) {
		return result, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("fan-out completed with %d failures and %d cancellations", result.Failed, result.Canceled),
			"FanOutAggregateFailure",
			nil,
			result,
		)
	}
	return result, nil
}

func classifyActivityFailure(err error) ActivityFailure {
	var applicationErr *temporal.ApplicationError
	if errors.As(err, &applicationErr) {
		return ActivityFailure{
			Kind:         ApplicationFailure,
			Message:      applicationErr.Message(),
			Type:         applicationErr.Type(),
			NonRetryable: applicationErr.NonRetryable(),
		}
	}

	var timeoutErr *temporal.TimeoutError
	if errors.As(err, &timeoutErr) {
		return ActivityFailure{
			Kind:        TimeoutFailure,
			Message:     timeoutErr.Message(),
			TimeoutType: timeoutErr.TimeoutType().String(),
		}
	}

	var canceledErr *temporal.CanceledError
	if errors.As(err, &canceledErr) {
		return ActivityFailure{Kind: CanceledFailure, Message: canceledErr.Error()}
	}

	var panicErr *temporal.PanicError
	if errors.As(err, &panicErr) {
		return ActivityFailure{Kind: PanicFailure, Message: panicErr.Error()}
	}

	return ActivityFailure{Kind: UnknownFailure, Message: err.Error()}
}
