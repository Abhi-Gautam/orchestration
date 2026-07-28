package workflows

import (
	"fmt"
	"sort"
	"time"

	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orchestrationv1 "orchestration/gen/orchestration/v1"
	"orchestration/internal/activities"
)

const DynamicFanOutWorkflowName = "DynamicFanOutWorkflow"

func DynamicFanOutWorkflow(ctx workflow.Context, input *orchestrationv1.DynamicFanOutRequest) (*orchestrationv1.DynamicFanOutResult, error) {
	if input == nil || input.RequestedCount <= 0 {
		return nil, invalidRequest("INVALID_DYNAMIC_FAN_OUT_REQUEST", "requested_count must be greater than zero")
	}
	branchDuration := duration(input.BranchDuration)
	finalizeDuration := duration(input.FinalizeDuration)
	if branchDuration < 0 || finalizeDuration < 0 {
		return nil, invalidRequest("INVALID_DYNAMIC_FAN_OUT_REQUEST", "durations must not be negative")
	}

	planned := int64(input.RequestedCount) + 2
	status, err := newStatusTracker(ctx, "planning", "plan-fan-out", "Planning dynamic fan-out", operationProgress(planned, 1, 1, 0, 0, 0))
	if err != nil {
		return nil, err
	}

	startedAt := workflow.Now(ctx)
	planCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ActivityID: "plan-fan-out", StartToCloseTimeout: 10 * time.Second})
	var plan activities.FanOutPlanResult
	if err := workflow.ExecuteActivity(planCtx, activities.PlanFanOutActivity, activities.FanOutPlanInput{Count: int(input.RequestedCount)}).Get(ctx, &plan); err != nil {
		return nil, err
	}

	planned = int64(plan.Count) + 2
	futures := make([]workflow.Future, plan.Count)
	for i := 0; i < plan.Count; i++ {
		futures[i] = scheduleWaitActivity(ctx, fmt.Sprintf("branch-%04d", i), branchDuration)
	}
	status.setRunning("fan-out", "branches", "Running dynamic fan-out branches", operationProgress(planned, int64(plan.Count)+1, int64(plan.Count), 1, 0, 0))
	branchResults := make([]activities.WaitResult, plan.Count)
	for i, future := range futures {
		if err := future.Get(ctx, &branchResults[i]); err != nil {
			return nil, err
		}
		status.setRunning("fan-out", fmt.Sprintf("branch-%04d", i), "Collecting dynamic fan-out results", operationProgress(planned, int64(plan.Count)+1, int64(plan.Count-i-1), int64(i)+2, 0, 0))
	}
	status.setRunning("finalizing", "finalize", "Finalizing dynamic fan-out", operationProgress(planned, planned, 1, int64(plan.Count)+1, 0, 0))
	var finalize activities.WaitResult
	if err := scheduleWaitActivity(ctx, "finalize", finalizeDuration).Get(ctx, &finalize); err != nil {
		return nil, err
	}

	finishedAt := workflow.Now(ctx)
	status.setSucceeded("completed", "Dynamic fan-out completed", operationProgress(planned, planned, 0, planned, 0, 0))
	firstStartedAt, lastFinishedAt, peakConcurrency := summarizeBranchTiming(branchResults)
	return &orchestrationv1.DynamicFanOutResult{
		PlannedCount:           int32(plan.Count),
		CompletedCount:         int32(len(branchResults)),
		StartedAt:              timestamppb.New(startedAt),
		FinishedAt:             timestamppb.New(finishedAt),
		Elapsed:                durationpb.New(finishedAt.Sub(startedAt)),
		FirstBranchStartedAt:   timestamppb.New(firstStartedAt),
		LastBranchFinishedAt:   timestamppb.New(lastFinishedAt),
		PeakConcurrentBranches: int32(peakConcurrency),
		Finalize:               waitResult(finalize),
	}, nil
}

type timingEvent struct {
	at    time.Time
	delta int
	name  string
}

func summarizeBranchTiming(results []activities.WaitResult) (time.Time, time.Time, int) {
	if len(results) == 0 {
		return time.Time{}, time.Time{}, 0
	}
	firstStartedAt, lastFinishedAt := results[0].StartedAt, results[0].FinishedAt
	events := make([]timingEvent, 0, len(results)*2)
	for _, result := range results {
		if result.StartedAt.Before(firstStartedAt) {
			firstStartedAt = result.StartedAt
		}
		if result.FinishedAt.After(lastFinishedAt) {
			lastFinishedAt = result.FinishedAt
		}
		events = append(events, timingEvent{at: result.StartedAt, delta: 1, name: result.Name}, timingEvent{at: result.FinishedAt, delta: -1, name: result.Name})
	}
	sort.Slice(events, func(i, j int) bool {
		if !events[i].at.Equal(events[j].at) {
			return events[i].at.Before(events[j].at)
		}
		if events[i].delta != events[j].delta {
			return events[i].delta < events[j].delta
		}
		return events[i].name < events[j].name
	})
	current, peak := 0, 0
	for _, event := range events {
		current += event.delta
		if current > peak {
			peak = current
		}
	}
	return firstStartedAt, lastFinishedAt, peak
}
