package workflows

import (
	"fmt"
	"sort"
	"time"

	"go.temporal.io/sdk/workflow"

	"orchestration/internal/activities"
)

const DynamicFanOutWorkflowName = "DynamicFanOutWorkflow"

type DynamicFanOutInput struct {
	RequestedCount   int
	BranchDuration   time.Duration
	FinalizeDuration time.Duration
}

type DynamicFanOutResult struct {
	PlannedCount           int
	CompletedCount         int
	StartedAt              time.Time
	FinishedAt             time.Time
	Elapsed                time.Duration
	FirstBranchStartedAt   time.Time
	LastBranchFinishedAt   time.Time
	PeakConcurrentBranches int
	Finalize               activities.WaitResult
}

func DynamicFanOutWorkflow(ctx workflow.Context, input DynamicFanOutInput) (DynamicFanOutResult, error) {
	startedAt := workflow.Now(ctx)

	planCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:          "plan-fan-out",
		StartToCloseTimeout: 10 * time.Second,
	})
	var plan activities.FanOutPlanResult
	if err := workflow.ExecuteActivity(planCtx, activities.PlanFanOutActivity, activities.FanOutPlanInput{
		Count: input.RequestedCount,
	}).Get(ctx, &plan); err != nil {
		return DynamicFanOutResult{}, err
	}

	futures := make([]workflow.Future, plan.Count)
	for i := 0; i < plan.Count; i++ {
		name := fmt.Sprintf("branch-%04d", i)
		futures[i] = scheduleWaitActivity(ctx, name, input.BranchDuration)
	}

	branchResults := make([]activities.WaitResult, plan.Count)
	for i, future := range futures {
		if err := future.Get(ctx, &branchResults[i]); err != nil {
			return DynamicFanOutResult{}, err
		}
	}

	var finalize activities.WaitResult
	if err := scheduleWaitActivity(ctx, "finalize", input.FinalizeDuration).Get(ctx, &finalize); err != nil {
		return DynamicFanOutResult{}, err
	}

	finishedAt := workflow.Now(ctx)
	firstStartedAt, lastFinishedAt, peakConcurrency := summarizeBranchTiming(branchResults)
	return DynamicFanOutResult{
		PlannedCount:           plan.Count,
		CompletedCount:         len(branchResults),
		StartedAt:              startedAt,
		FinishedAt:             finishedAt,
		Elapsed:                finishedAt.Sub(startedAt),
		FirstBranchStartedAt:   firstStartedAt,
		LastBranchFinishedAt:   lastFinishedAt,
		PeakConcurrentBranches: peakConcurrency,
		Finalize:               finalize,
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

	firstStartedAt := results[0].StartedAt
	lastFinishedAt := results[0].FinishedAt
	events := make([]timingEvent, 0, len(results)*2)
	for _, result := range results {
		if result.StartedAt.Before(firstStartedAt) {
			firstStartedAt = result.StartedAt
		}
		if result.FinishedAt.After(lastFinishedAt) {
			lastFinishedAt = result.FinishedAt
		}
		events = append(events,
			timingEvent{at: result.StartedAt, delta: 1, name: result.Name},
			timingEvent{at: result.FinishedAt, delta: -1, name: result.Name},
		)
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
