package workflows

import (
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orchestrationv1 "orchestration/gen/orchestration/v1"
	"orchestration/internal/activities"
)

func SimpleDiamondWorkflow(ctx workflow.Context, input *orchestrationv1.SimpleDiamondRequest) (*orchestrationv1.SimpleDiamondResult, error) {
	if input == nil {
		return nil, invalidRequest("INVALID_SIMPLE_DIAMOND_REQUEST", "input is required")
	}
	prepareDuration := duration(input.PrepareDuration)
	branchADuration := duration(input.BranchADuration)
	branchBDuration := duration(input.BranchBDuration)
	finalizeDuration := duration(input.FinalizeDuration)
	if prepareDuration < 0 || branchADuration < 0 || branchBDuration < 0 || finalizeDuration < 0 {
		return nil, invalidRequest("INVALID_SIMPLE_DIAMOND_REQUEST", "durations must not be negative")
	}

	status, err := newStatusTracker(ctx, 4, "preparing", "prepare", "Preparing diamond workflow")
	if err != nil {
		return nil, err
	}

	startedAt := workflow.Now(ctx)
	status.scheduleWork(1)
	var prepare activities.WaitResult
	if err := scheduleWaitActivity(ctx, "prepare", prepareDuration).Get(ctx, &prepare); err != nil {
		return nil, err
	}
	status.recordSucceeded()
	status.scheduleWork(2)
	branchAFuture := scheduleWaitActivity(ctx, "branch-a", branchADuration)
	branchBFuture := scheduleWaitActivity(ctx, "branch-b", branchBDuration)
	status.setRunning("branching", "branch-a,branch-b", "Running parallel branches")
	var branchA activities.WaitResult
	if err := branchAFuture.Get(ctx, &branchA); err != nil {
		return nil, err
	}
	status.recordSucceeded()
	status.setRunning("branching", "branch-b", "Waiting for parallel branches")
	var branchB activities.WaitResult
	if err := branchBFuture.Get(ctx, &branchB); err != nil {
		return nil, err
	}
	status.recordSucceeded()
	status.scheduleWork(1)
	status.setRunning("finalizing", "finalize", "Finalizing diamond workflow")
	var finalize activities.WaitResult
	if err := scheduleWaitActivity(ctx, "finalize", finalizeDuration).Get(ctx, &finalize); err != nil {
		return nil, err
	}
	finishedAt := workflow.Now(ctx)
	status.recordSucceeded()
	status.setSucceeded("completed", "Diamond workflow completed")

	return &orchestrationv1.SimpleDiamondResult{
		StartedAt:  timestamppb.New(startedAt),
		FinishedAt: timestamppb.New(finishedAt),
		Elapsed:    durationpb.New(finishedAt.Sub(startedAt)),
		Nodes:      waitResults([]activities.WaitResult{prepare, branchA, branchB, finalize}),
	}, nil
}
