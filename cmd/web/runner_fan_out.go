package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"

	"orchestration/internal/activities"
	"orchestration/internal/workflows"
)

func (s *server) runFanOutPolicy(ctx context.Context, raw json.RawMessage, started time.Time) (*runResponse, error) {
	var input fanOutPolicyWebInput
	if err := decodeInput(raw, &input); err != nil {
		return nil, err
	}
	policy := workflows.AggregationPolicy(input.Policy)
	if policy != workflows.FailFast && policy != workflows.AllSettled && policy != workflows.AllSettledThenFail {
		return nil, &inputError{message: "Input field \"policy\" must be fail-fast, all-settled, or all-settled-then-fail."}
	}
	if input.BranchCount <= 0 || input.BranchCount > 100 {
		return nil, &inputError{message: "Input field \"branchCount\" must be between 1 and 100."}
	}
	faultMode := activities.FaultMode(input.FaultMode)
	if faultMode == "" {
		faultMode = activities.FaultNonRetryableFailure
	}
	if !validFaultMode(faultMode) {
		return nil, &inputError{message: fmt.Sprintf("Unsupported faultMode %q.", input.FaultMode)}
	}
	failureBranch := -1
	if input.FailureBranch != nil {
		failureBranch = *input.FailureBranch
		if failureBranch < 0 || failureBranch >= input.BranchCount {
			return nil, &inputError{message: "Input field \"failureBranch\" must be between 0 and branchCount-1."}
		}
	}
	branchDuration := input.BranchDuration.Duration()
	if branchDuration == 0 {
		branchDuration = 2 * time.Second
	}
	if branchDuration < 0 {
		return nil, &inputError{message: "Durations must not be negative."}
	}
	finalizeDuration := input.FinalizeDuration.Duration()
	if input.FinalizeDuration == 0 && !jsonHasField(raw, "finalizeDuration") {
		finalizeDuration = 500 * time.Millisecond
	}
	if finalizeDuration < 0 {
		return nil, &inputError{message: "Durations must not be negative."}
	}
	workflowInput := workflows.FanOutPolicyInput{Policy: policy, Branches: buildFaultBranches(input.BranchCount, failureBranch, faultMode, branchDuration), FinalizeDuration: finalizeDuration}
	run, err := s.start(ctx, "fan-out-"+string(policy), workflows.FanOutPolicyWorkflowName, workflowInput)
	if err != nil {
		return nil, err
	}
	var result workflows.FanOutResult
	workflowErr := run.Get(ctx, &result)
	return s.buildResponse("fan-out-policy", run, started, result, workflowErr), nil
}
func validFaultMode(mode activities.FaultMode) bool {
	switch mode {
	case activities.FaultSuccess, activities.FaultRetryableFailure, activities.FaultNonRetryableFailure, activities.FaultPanic, activities.FaultStartToCloseTimeout, activities.FaultHeartbeatTimeout, activities.FaultWaitForCancellation:
		return true
	}
	return false
}
func buildFaultBranches(count, failureBranch int, faultMode activities.FaultMode, duration time.Duration) []workflows.FaultBranch {
	branches := make([]workflows.FaultBranch, count)
	for i := range branches {
		id := fmt.Sprintf("branch-%02d", i)
		mode := activities.FaultSuccess
		workDuration := duration
		if i == failureBranch {
			mode = faultMode
			workDuration = 250 * time.Millisecond
		}
		branches[i] = workflows.FaultBranch{ActivityID: id, Input: activities.FaultActivityInput{Name: id, Mode: mode, WorkDuration: workDuration, HeartbeatInterval: 100 * time.Millisecond}, StartToCloseTimeout: 5 * time.Second, ScheduleToCloseTimeout: 10 * time.Second, HeartbeatTimeout: time.Second, RetryPolicy: temporal.RetryPolicy{InitialInterval: 100 * time.Millisecond, BackoffCoefficient: 2, MaximumInterval: time.Second, MaximumAttempts: 1}}
	}
	return branches
}
