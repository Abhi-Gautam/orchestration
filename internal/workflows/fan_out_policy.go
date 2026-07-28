package workflows

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orchestrationv1 "orchestration/gen/orchestration/v1"
	"orchestration/internal/activities"
)

const FanOutPolicyWorkflowName = "FanOutPolicyWorkflow"

type scheduledActivity struct {
	index      int
	activityID string
	future     workflow.Future
}

func FanOutPolicyWorkflow(ctx workflow.Context, input *orchestrationv1.FanOutPolicyRequest) (*orchestrationv1.FanOutPolicyResult, error) {
	if err := validateFanOutPolicyRequest(input); err != nil {
		return nil, invalidRequest("INVALID_FAN_OUT_POLICY_REQUEST", err.Error())
	}

	finalizeDuration := duration(input.FinalizeDuration)
	planned := int64(len(input.Branches))
	if finalizeDuration > 0 {
		planned++
	}
	status, err := newStatusTracker(ctx, "fan-out", "branches", "Running fan-out policy branches", operationProgress(planned, int64(len(input.Branches)), int64(len(input.Branches)), 0, 0, 0))
	if err != nil {
		return nil, err
	}

	startedAt := workflow.Now(ctx)
	activityCtx, cancelActivities := workflow.WithCancel(ctx)
	defer cancelActivities()

	scheduled := scheduleFaultActivities(activityCtx, input.Branches)
	result, err := aggregateFaultActivities(ctx, scheduled, input.Policy, cancelActivities, status, planned)
	result.Policy = input.Policy
	result.StartedAt = timestamppb.New(startedAt)
	if err != nil {
		finishFanOutResult(ctx, result, startedAt)
		return result, err
	}

	if finalizeDuration > 0 {
		status.setRunning("finalizing", "finalize", "Finalizing fan-out policy", operationProgress(planned, planned, 1, int64(result.Succeeded), int64(result.Failed), int64(result.Canceled)))
		var finalize activities.WaitResult
		if err := scheduleWaitActivity(ctx, "finalize", finalizeDuration).Get(ctx, &finalize); err != nil {
			return result, err
		}
		result.Finalize = waitResult(finalize)
	}
	finishFanOutResult(ctx, result, startedAt)
	succeeded := int64(result.Succeeded)
	if finalizeDuration > 0 {
		succeeded++
	}
	status.setSucceeded("completed", "Fan-out policy completed", operationProgress(planned, planned, 0, succeeded, int64(result.Failed), int64(result.Canceled)))
	return result, nil
}

func validateFanOutPolicyRequest(input *orchestrationv1.FanOutPolicyRequest) error {
	if input == nil {
		return errors.New("input is required")
	}
	switch input.Policy {
	case orchestrationv1.AggregationPolicy_AGGREGATION_POLICY_FAIL_FAST,
		orchestrationv1.AggregationPolicy_AGGREGATION_POLICY_ALL_SETTLED,
		orchestrationv1.AggregationPolicy_AGGREGATION_POLICY_ALL_SETTLED_THEN_FAIL:
	default:
		return errors.New("policy is required")
	}
	if len(input.Branches) == 0 {
		return errors.New("at least one branch is required")
	}
	if duration(input.FinalizeDuration) < 0 {
		return errors.New("finalize_duration must not be negative")
	}
	for i, branch := range input.Branches {
		if branch == nil || branch.Name == "" {
			return fmt.Errorf("branch %d requires a name", i)
		}
		if _, ok := activityFaultMode(branch.Mode); !ok {
			return fmt.Errorf("branch %q has an unsupported fault mode", branch.Name)
		}
		if duration(branch.WorkDuration) < 0 || duration(branch.StallDuration) < 0 || duration(branch.HeartbeatInterval) < 0 {
			return fmt.Errorf("branch %q durations must not be negative", branch.Name)
		}
	}
	return nil
}

func scheduleFaultActivities(ctx workflow.Context, branches []*orchestrationv1.FaultBranchSpec) []scheduledActivity {
	scheduled := make([]scheduledActivity, len(branches))
	for i, branch := range branches {
		mode, _ := activityFaultMode(branch.Mode)
		options := workflow.ActivityOptions{
			ActivityID:             branch.Name,
			StartToCloseTimeout:    5 * time.Second,
			ScheduleToCloseTimeout: 10 * time.Second,
			HeartbeatTimeout:       time.Second,
			WaitForCancellation:    true,
			RetryPolicy:            &temporal.RetryPolicy{InitialInterval: 100 * time.Millisecond, BackoffCoefficient: 2, MaximumInterval: time.Second, MaximumAttempts: 1},
		}
		activityCtx := workflow.WithActivityOptions(ctx, options)
		scheduled[i] = scheduledActivity{index: i, activityID: branch.Name, future: workflow.ExecuteActivity(activityCtx, activities.FaultInjectionActivity, activities.FaultActivityInput{
			Name: branch.Name, Mode: mode, WorkDuration: duration(branch.WorkDuration), StallDuration: duration(branch.StallDuration), Seed: branch.Seed, FailUntilAttempt: branch.FailUntilAttempt, HeartbeatInterval: duration(branch.HeartbeatInterval),
		})}
	}
	return scheduled
}

func aggregateFaultActivities(ctx workflow.Context, scheduled []scheduledActivity, policy orchestrationv1.AggregationPolicy, cancelActivities workflow.CancelFunc, status *statusTracker, planned int64) (*orchestrationv1.FanOutPolicyResult, error) {
	result := &orchestrationv1.FanOutPolicyResult{Planned: int32(len(scheduled)), Outcomes: make([]*orchestrationv1.ActivityOutcome, len(scheduled))}
	selector := workflow.NewSelector(ctx)
	remaining := len(scheduled)
	var firstErr error
	for _, item := range scheduled {
		item := item
		selector.AddFuture(item.future, func(f workflow.Future) {
			outcome := &orchestrationv1.ActivityOutcome{ActivityId: item.activityID, Index: int32(item.index)}
			var activityResult activities.FaultActivityResult
			if err := f.Get(ctx, &activityResult); err != nil {
				outcome.Failure = classifyActivityFailure(err)
				if outcome.Failure.Kind == orchestrationv1.ActivityFailureKind_ACTIVITY_FAILURE_KIND_CANCELED {
					result.Canceled++
				} else {
					result.Failed++
				}
				if policy == orchestrationv1.AggregationPolicy_AGGREGATION_POLICY_FAIL_FAST && firstErr == nil {
					firstErr = err
					cancelActivities()
				}
			} else {
				outcome.Result = faultActivityResult(activityResult)
				result.Succeeded++
			}
			result.Outcomes[item.index] = outcome
			remaining--
			status.setRunning("fan-out", item.activityID, "Collecting fan-out policy outcomes", operationProgress(planned, int64(len(scheduled)), int64(remaining), int64(result.Succeeded), int64(result.Failed), int64(result.Canceled)))
		})
	}
	for remaining > 0 {
		selector.Select(ctx)
	}
	if firstErr != nil {
		return result, firstErr
	}
	if policy == orchestrationv1.AggregationPolicy_AGGREGATION_POLICY_ALL_SETTLED_THEN_FAIL && (result.Failed > 0 || result.Canceled > 0) {
		message := fmt.Sprintf("fan-out completed with %d failures and %d cancellations", result.Failed, result.Canceled)
		failure := &orchestrationv1.WorkflowFailure{Code: "FAN_OUT_AGGREGATE_FAILURE", Message: message, Category: orchestrationv1.FailureCategory_FAILURE_CATEGORY_BUSINESS}
		return result, temporal.NewNonRetryableApplicationError(message, "FanOutAggregateFailure", nil, failure, result)
	}
	return result, nil
}

func finishFanOutResult(ctx workflow.Context, result *orchestrationv1.FanOutPolicyResult, startedAt time.Time) {
	finishedAt := workflow.Now(ctx)
	result.FinishedAt = timestamppb.New(finishedAt)
	result.Elapsed = durationpb.New(finishedAt.Sub(startedAt))
}

func classifyActivityFailure(err error) *orchestrationv1.ActivityFailure {
	var applicationErr *temporal.ApplicationError
	if errors.As(err, &applicationErr) {
		return &orchestrationv1.ActivityFailure{Kind: orchestrationv1.ActivityFailureKind_ACTIVITY_FAILURE_KIND_APPLICATION, Message: applicationErr.Message(), Type: applicationErr.Type(), NonRetryable: applicationErr.NonRetryable()}
	}
	var timeoutErr *temporal.TimeoutError
	if errors.As(err, &timeoutErr) {
		return &orchestrationv1.ActivityFailure{Kind: orchestrationv1.ActivityFailureKind_ACTIVITY_FAILURE_KIND_TIMEOUT, Message: timeoutErr.Message(), TimeoutType: timeoutErr.TimeoutType().String()}
	}
	var canceledErr *temporal.CanceledError
	if errors.As(err, &canceledErr) {
		return &orchestrationv1.ActivityFailure{Kind: orchestrationv1.ActivityFailureKind_ACTIVITY_FAILURE_KIND_CANCELED, Message: canceledErr.Error()}
	}
	var panicErr *temporal.PanicError
	if errors.As(err, &panicErr) {
		return &orchestrationv1.ActivityFailure{Kind: orchestrationv1.ActivityFailureKind_ACTIVITY_FAILURE_KIND_PANIC, Message: panicErr.Error()}
	}
	return &orchestrationv1.ActivityFailure{Kind: orchestrationv1.ActivityFailureKind_ACTIVITY_FAILURE_KIND_UNKNOWN, Message: err.Error()}
}

func activityFaultMode(mode orchestrationv1.FaultMode) (activities.FaultMode, bool) {
	switch mode {
	case orchestrationv1.FaultMode_FAULT_MODE_SUCCESS:
		return activities.FaultSuccess, true
	case orchestrationv1.FaultMode_FAULT_MODE_RETRYABLE_FAILURE:
		return activities.FaultRetryableFailure, true
	case orchestrationv1.FaultMode_FAULT_MODE_NON_RETRYABLE_FAILURE:
		return activities.FaultNonRetryableFailure, true
	case orchestrationv1.FaultMode_FAULT_MODE_PANIC:
		return activities.FaultPanic, true
	case orchestrationv1.FaultMode_FAULT_MODE_START_TO_CLOSE_TIMEOUT:
		return activities.FaultStartToCloseTimeout, true
	case orchestrationv1.FaultMode_FAULT_MODE_HEARTBEAT_TIMEOUT:
		return activities.FaultHeartbeatTimeout, true
	case orchestrationv1.FaultMode_FAULT_MODE_WAIT_FOR_CANCELLATION:
		return activities.FaultWaitForCancellation, true
	default:
		return "", false
	}
}

func protoFaultMode(mode activities.FaultMode) orchestrationv1.FaultMode {
	switch mode {
	case activities.FaultSuccess:
		return orchestrationv1.FaultMode_FAULT_MODE_SUCCESS
	case activities.FaultRetryableFailure:
		return orchestrationv1.FaultMode_FAULT_MODE_RETRYABLE_FAILURE
	case activities.FaultNonRetryableFailure:
		return orchestrationv1.FaultMode_FAULT_MODE_NON_RETRYABLE_FAILURE
	case activities.FaultPanic:
		return orchestrationv1.FaultMode_FAULT_MODE_PANIC
	case activities.FaultStartToCloseTimeout:
		return orchestrationv1.FaultMode_FAULT_MODE_START_TO_CLOSE_TIMEOUT
	case activities.FaultHeartbeatTimeout:
		return orchestrationv1.FaultMode_FAULT_MODE_HEARTBEAT_TIMEOUT
	case activities.FaultWaitForCancellation:
		return orchestrationv1.FaultMode_FAULT_MODE_WAIT_FOR_CANCELLATION
	default:
		return orchestrationv1.FaultMode_FAULT_MODE_UNSPECIFIED
	}
}

func faultActivityResult(result activities.FaultActivityResult) *orchestrationv1.FaultActivityResult {
	return &orchestrationv1.FaultActivityResult{Name: result.Name, Outcome: protoFaultMode(result.Outcome), Attempt: result.Attempt, StartedAt: timestamppb.New(result.StartedAt), FinishedAt: timestamppb.New(result.FinishedAt), Elapsed: durationpb.New(result.Elapsed)}
}
