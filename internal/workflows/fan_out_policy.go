package workflows

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orchestrationv1 "orchestration/gen/orchestration/v1"
	"orchestration/internal/activities"
)

const (
	FanOutPolicyWorkflowName = "FanOutPolicyWorkflow"
	maxFanOutSamples         = 8
)

type scheduledActivity struct {
	index      int
	activityID string
	future     workflow.Future
}

type fanOutCollector struct {
	result         *orchestrationv1.FanOutPolicyResult
	successDigests [][]byte
	sampleKeys     map[string]struct{}
}

func FanOutPolicyWorkflow(ctx workflow.Context, input *orchestrationv1.FanOutPolicyRequest) (*orchestrationv1.FanOutPolicyResult, error) {
	branches, campaignType, seed, err := resolveFaultBranches(input)
	if err != nil {
		return nil, invalidRequest("INVALID_FAN_OUT_POLICY_REQUEST", err.Error())
	}

	planned := int64(len(branches))
	status, err := newStatusTracker(
		ctx,
		"fan-out",
		"scheduling",
		"Scheduling fan-out policy Activities",
		operationProgress(planned, planned, planned, 0, 0, 0),
	)
	if err != nil {
		return nil, err
	}

	startedAt := workflow.Now(ctx)
	activityCtx, cancelActivities := workflow.WithCancel(ctx)
	defer cancelActivities()

	scheduled := scheduleFaultActivities(activityCtx, branches)
	result, firstTerminalErr := aggregateFaultActivities(ctx, scheduled, input.Policy, cancelActivities, status)
	result.Policy = input.Policy
	result.CampaignType = campaignType
	result.Seed = seed
	result.StartedAt = timestamppb.New(startedAt)
	finishFanOutResult(ctx, result, startedAt)

	progress := operationProgress(planned, planned, 0, int64(result.Succeeded), int64(result.Failed), int64(result.Canceled))
	if ctx.Err() != nil {
		status.setCanceled("canceled", "Fan-out policy was canceled", progress)
		return result, ctx.Err()
	}
	if firstTerminalErr != nil {
		status.setFailed("failed", "Fan-out policy stopped after a terminal Activity failure", progress)
		return result, failFastWorkflowError(result)
	}
	if input.Policy == orchestrationv1.AggregationPolicy_AGGREGATION_POLICY_ALL_SETTLED_THEN_FAIL && (result.Failed > 0 || result.Canceled > 0) {
		status.setFailed("failed", "Fan-out policy aggregated available outputs and failed", progress)
		return result, aggregateWorkflowError(result)
	}

	message := "Fan-out policy completed"
	if result.Failed > 0 || result.Canceled > 0 {
		message = "Fan-out policy completed with a partial aggregate"
	}
	status.setSucceeded(
		"completed",
		message,
		progress,
	)
	return result, nil
}

func scheduleFaultActivities(ctx workflow.Context, branches []*orchestrationv1.FaultBranchSpec) []scheduledActivity {
	scheduled := make([]scheduledActivity, len(branches))
	for i, branch := range branches {
		mode, _ := activityFaultMode(branch.Mode)
		options := workflow.ActivityOptions{
			ActivityID:             branch.Name,
			StartToCloseTimeout:    2 * time.Second,
			ScheduleToCloseTimeout: 2 * time.Minute,
			HeartbeatTimeout:       500 * time.Millisecond,
			WaitForCancellation:    true,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    100 * time.Millisecond,
				BackoffCoefficient: 2,
				MaximumInterval:    500 * time.Millisecond,
				MaximumAttempts:    3,
			},
		}
		activityCtx := workflow.WithActivityOptions(ctx, options)
		scheduled[i] = scheduledActivity{
			index:      i,
			activityID: branch.Name,
			future: workflow.ExecuteActivity(activityCtx, activities.FaultInjectionActivity, activities.FaultActivityInput{
				Name:              branch.Name,
				Mode:              mode,
				WorkDuration:      duration(branch.WorkDuration),
				StallDuration:     duration(branch.StallDuration),
				Seed:              branch.Seed,
				Probabilities:     activityOutcomeProbabilities(branch.Probabilities),
				FailUntilAttempt:  branch.FailUntilAttempt,
				HeartbeatInterval: duration(branch.HeartbeatInterval),
			}),
		}
	}
	return scheduled
}

func aggregateFaultActivities(
	ctx workflow.Context,
	scheduled []scheduledActivity,
	policy orchestrationv1.AggregationPolicy,
	cancelActivities workflow.CancelFunc,
	status *statusTracker,
) (*orchestrationv1.FanOutPolicyResult, error) {
	collector := &fanOutCollector{
		result: &orchestrationv1.FanOutPolicyResult{
			Planned:          int32(len(scheduled)),
			FailureBreakdown: &orchestrationv1.FanOutFailureBreakdown{},
		},
		successDigests: make([][]byte, len(scheduled)),
		sampleKeys:     make(map[string]struct{}, maxFanOutSamples),
	}

	selector := workflow.NewSelector(ctx)
	remaining := len(scheduled)
	var firstTerminalErr error
	for _, item := range scheduled {
		item := item
		selector.AddFuture(item.future, func(f workflow.Future) {
			outcome := &orchestrationv1.ActivityOutcome{ActivityId: item.activityID, Index: int32(item.index)}
			var activityResult activities.FaultActivityResult
			if err := f.Get(ctx, &activityResult); err != nil {
				outcome.Failure = classifyActivityFailure(err)
				collector.recordFailure(outcome)
				if policy == orchestrationv1.AggregationPolicy_AGGREGATION_POLICY_FAIL_FAST && firstTerminalErr == nil && outcome.Failure.Kind != orchestrationv1.ActivityFailureKind_ACTIVITY_FAILURE_KIND_CANCELED {
					firstTerminalErr = err
					collector.result.FailFastTrigger = outcome
					cancelActivities()
				}
			} else {
				outcome.Result = faultActivityResult(activityResult)
				collector.recordSuccess(item.index, outcome, activityResult)
			}

			remaining--
			status.setRunning(
				"fan-out",
				item.activityID,
				"Collecting fan-out policy outcomes",
				operationProgress(
					int64(len(scheduled)),
					int64(len(scheduled)),
					int64(remaining),
					int64(collector.result.Succeeded),
					int64(collector.result.Failed),
					int64(collector.result.Canceled),
				),
			)
		})
	}

	for remaining > 0 {
		selector.Select(ctx)
	}
	if policy != orchestrationv1.AggregationPolicy_AGGREGATION_POLICY_FAIL_FAST || collector.result.Succeeded == collector.result.Planned {
		collector.result.Aggregate = collector.completeAggregate()
	}
	return collector.result, firstTerminalErr
}

func (collector *fanOutCollector) recordSuccess(index int, outcome *orchestrationv1.ActivityOutcome, result activities.FaultActivityResult) {
	collector.result.Succeeded++
	if result.Attempt > 1 {
		collector.result.SucceededAfterRetry++
		collector.addSample("succeeded-after-retry", outcome)
	} else {
		collector.result.SucceededFirstAttempt++
	}
	collector.successDigests[index] = successfulResultDigest(index, outcome.ActivityId, result)
}

func (collector *fanOutCollector) recordFailure(outcome *orchestrationv1.ActivityOutcome) {
	failure := outcome.Failure
	key := "unknown"
	switch {
	case failure.Kind == orchestrationv1.ActivityFailureKind_ACTIVITY_FAILURE_KIND_CANCELED:
		collector.result.Canceled++
		collector.result.FailureBreakdown.Canceled++
		key = "canceled"
	case failure.Kind == orchestrationv1.ActivityFailureKind_ACTIVITY_FAILURE_KIND_PANIC:
		collector.result.Failed++
		collector.result.FailureBreakdown.Panic++
		key = "panic"
	case failure.Kind == orchestrationv1.ActivityFailureKind_ACTIVITY_FAILURE_KIND_TIMEOUT && failure.TimeoutType == enumspb.TIMEOUT_TYPE_START_TO_CLOSE.String():
		collector.result.Failed++
		collector.result.FailureBreakdown.StartToCloseTimeout++
		key = "start-to-close-timeout"
	case failure.Kind == orchestrationv1.ActivityFailureKind_ACTIVITY_FAILURE_KIND_TIMEOUT && failure.TimeoutType == enumspb.TIMEOUT_TYPE_HEARTBEAT.String():
		collector.result.Failed++
		collector.result.FailureBreakdown.HeartbeatTimeout++
		key = "heartbeat-timeout"
	case failure.Kind == orchestrationv1.ActivityFailureKind_ACTIVITY_FAILURE_KIND_APPLICATION && failure.Type == "InjectedRetryableFailure":
		collector.result.Failed++
		collector.result.FailureBreakdown.RetryExhausted++
		key = "retry-exhausted"
	case failure.Kind == orchestrationv1.ActivityFailureKind_ACTIVITY_FAILURE_KIND_APPLICATION && failure.NonRetryable:
		collector.result.Failed++
		collector.result.FailureBreakdown.NonRetryable++
		key = "non-retryable"
	default:
		collector.result.Failed++
		collector.result.FailureBreakdown.Unknown++
	}
	collector.addSample(key, outcome)
}

func (collector *fanOutCollector) addSample(key string, outcome *orchestrationv1.ActivityOutcome) {
	if len(collector.result.Samples) >= maxFanOutSamples {
		return
	}
	if _, exists := collector.sampleKeys[key]; exists {
		return
	}
	collector.sampleKeys[key] = struct{}{}
	collector.result.Samples = append(collector.result.Samples, outcome)
}

func (collector *fanOutCollector) completeAggregate() *orchestrationv1.FanOutAggregate {
	combined := sha256.New()
	consumed := int32(0)
	for _, digest := range collector.successDigests {
		if len(digest) == 0 {
			continue
		}
		_, _ = combined.Write(digest)
		consumed++
	}
	return &orchestrationv1.FanOutAggregate{
		Complete:                  true,
		ConsumedSuccessfulOutputs: consumed,
		SemanticDigest:            hex.EncodeToString(combined.Sum(nil)),
	}
}

func successfulResultDigest(index int, activityID string, result activities.FaultActivityResult) []byte {
	digest := sha256.New()
	_, _ = fmt.Fprintf(digest, "%d\x00%s\x00%s\x00%s\x00%d", index, activityID, result.Name, result.Outcome, result.Attempt)
	return digest.Sum(nil)
}

func finishFanOutResult(ctx workflow.Context, result *orchestrationv1.FanOutPolicyResult, startedAt time.Time) {
	finishedAt := workflow.Now(ctx)
	result.FinishedAt = timestamppb.New(finishedAt)
	result.Elapsed = durationpb.New(finishedAt.Sub(startedAt))
}

func failFastWorkflowError(result *orchestrationv1.FanOutPolicyResult) error {
	trigger := result.FailFastTrigger
	message := "fan-out stopped after the first terminal Activity failure"
	metadata := map[string]string{}
	if trigger != nil && trigger.Failure != nil {
		message = fmt.Sprintf("fan-out stopped after Activity %s failed: %s", trigger.ActivityId, trigger.Failure.Message)
		metadata["activityId"] = trigger.ActivityId
		metadata["inputIndex"] = strconv.Itoa(int(trigger.Index))
		metadata["failureKind"] = trigger.Failure.Kind.String()
		metadata["failureType"] = trigger.Failure.Type
		metadata["timeoutType"] = trigger.Failure.TimeoutType
		if trigger.Failure.FinalAttempt > 0 {
			metadata["finalAttempt"] = strconv.Itoa(int(trigger.Failure.FinalAttempt))
		}
	}
	failure := &orchestrationv1.WorkflowFailure{
		Code:      "FAN_OUT_FAIL_FAST",
		Message:   message,
		Category:  orchestrationv1.FailureCategory_FAILURE_CATEGORY_DEPENDENCY,
		Retryable: false,
		Metadata:  metadata,
	}
	return temporal.NewNonRetryableApplicationError(message, "FanOutFailFast", nil, failure, result)
}

func aggregateWorkflowError(result *orchestrationv1.FanOutPolicyResult) error {
	message := fmt.Sprintf("fan-out completed with %d failures and %d cancellations", result.Failed, result.Canceled)
	failure := &orchestrationv1.WorkflowFailure{
		Code:      "FAN_OUT_AGGREGATE_FAILURE",
		Message:   message,
		Category:  orchestrationv1.FailureCategory_FAILURE_CATEGORY_BUSINESS,
		Retryable: false,
	}
	return temporal.NewNonRetryableApplicationError(message, "FanOutAggregateFailure", nil, failure, result)
}

func classifyActivityFailure(err error) *orchestrationv1.ActivityFailure {
	var canceledErr *temporal.CanceledError
	if errors.As(err, &canceledErr) {
		return &orchestrationv1.ActivityFailure{
			Kind:    orchestrationv1.ActivityFailureKind_ACTIVITY_FAILURE_KIND_CANCELED,
			Message: safeFailureMessage(canceledErr.Error()),
		}
	}

	var timeoutErr *temporal.TimeoutError
	if errors.As(err, &timeoutErr) {
		failure := &orchestrationv1.ActivityFailure{
			Kind:        orchestrationv1.ActivityFailureKind_ACTIVITY_FAILURE_KIND_TIMEOUT,
			Message:     safeFailureMessage(timeoutErr.Message()),
			TimeoutType: timeoutErr.TimeoutType().String(),
		}
		if timeoutErr.HasLastHeartbeatDetails() {
			var heartbeat string
			_ = timeoutErr.LastHeartbeatDetails(&heartbeat, &failure.FinalAttempt)
		}
		return failure
	}

	var panicErr *temporal.PanicError
	if errors.As(err, &panicErr) {
		return &orchestrationv1.ActivityFailure{
			Kind:    orchestrationv1.ActivityFailureKind_ACTIVITY_FAILURE_KIND_PANIC,
			Message: safeFailureMessage(panicErr.Error()),
			Type:    "PanicError",
		}
	}

	var applicationErr *temporal.ApplicationError
	if errors.As(err, &applicationErr) {
		failure := &orchestrationv1.ActivityFailure{
			Kind:         orchestrationv1.ActivityFailureKind_ACTIVITY_FAILURE_KIND_APPLICATION,
			Message:      safeFailureMessage(applicationErr.Message()),
			Type:         applicationErr.Type(),
			NonRetryable: applicationErr.NonRetryable(),
		}
		if applicationErr.HasDetails() {
			var activityName string
			_ = applicationErr.Details(&activityName, &failure.FinalAttempt)
		}
		return failure
	}

	return &orchestrationv1.ActivityFailure{
		Kind:    orchestrationv1.ActivityFailureKind_ACTIVITY_FAILURE_KIND_UNKNOWN,
		Message: safeFailureMessage(err.Error()),
	}
}

func safeFailureMessage(message string) string {
	message, _, _ = strings.Cut(message, "\n")
	const maxLength = 512
	if len(message) > maxLength {
		return message[:maxLength]
	}
	return message
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
	case orchestrationv1.FaultMode_FAULT_MODE_UNSPECIFIED:
		return "", true
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
	return &orchestrationv1.FaultActivityResult{
		Name:       result.Name,
		Outcome:    protoFaultMode(result.Outcome),
		Attempt:    result.Attempt,
		StartedAt:  timestamppb.New(result.StartedAt),
		FinishedAt: timestamppb.New(result.FinishedAt),
		Elapsed:    durationpb.New(result.Elapsed),
	}
}

func activityOutcomeProbabilities(probabilities *orchestrationv1.OutcomeProbabilities) *activities.OutcomeProbabilities {
	if probabilities == nil {
		return nil
	}
	return &activities.OutcomeProbabilities{
		Success:             int(probabilities.Success),
		RetryableFailure:    int(probabilities.RetryableFailure),
		NonRetryableFailure: int(probabilities.NonRetryableFailure),
		Panic:               int(probabilities.Panic),
		StartToCloseTimeout: int(probabilities.StartToCloseTimeout),
		HeartbeatTimeout:    int(probabilities.HeartbeatTimeout),
	}
}
