package workflows

import (
	"time"

	"go.temporal.io/sdk/workflow"

	"orchestration/internal/activities"
)

const SimpleDiamondWorkflowName = "SimpleDiamondWorkflow"

type SimpleDiamondInput struct {
	PrepareDuration  time.Duration
	BranchADuration  time.Duration
	BranchBDuration  time.Duration
	FinalizeDuration time.Duration
}

type SimpleDiamondResult struct {
	StartedAt  time.Time
	FinishedAt time.Time
	Elapsed    time.Duration
	Nodes      []activities.WaitResult
}

func SimpleDiamondWorkflow(ctx workflow.Context, input SimpleDiamondInput) (SimpleDiamondResult, error) {
	startedAt := workflow.Now(ctx)

	var prepare activities.WaitResult
	if err := scheduleWaitActivity(ctx, "prepare", input.PrepareDuration).Get(ctx, &prepare); err != nil {
		return SimpleDiamondResult{}, err
	}

	branchAFuture := scheduleWaitActivity(ctx, "branch-a", input.BranchADuration)
	branchBFuture := scheduleWaitActivity(ctx, "branch-b", input.BranchBDuration)

	var branchA activities.WaitResult
	if err := branchAFuture.Get(ctx, &branchA); err != nil {
		return SimpleDiamondResult{}, err
	}
	var branchB activities.WaitResult
	if err := branchBFuture.Get(ctx, &branchB); err != nil {
		return SimpleDiamondResult{}, err
	}

	var finalize activities.WaitResult
	if err := scheduleWaitActivity(ctx, "finalize", input.FinalizeDuration).Get(ctx, &finalize); err != nil {
		return SimpleDiamondResult{}, err
	}

	finishedAt := workflow.Now(ctx)
	return SimpleDiamondResult{
		StartedAt:  startedAt,
		FinishedAt: finishedAt,
		Elapsed:    finishedAt.Sub(startedAt),
		Nodes:      []activities.WaitResult{prepare, branchA, branchB, finalize},
	}, nil
}
