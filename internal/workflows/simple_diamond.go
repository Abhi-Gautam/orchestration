package workflows

import (
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orchestrationv1 "orchestration/gen/orchestration/v1"
	"orchestration/internal/activities"
)

const SimpleDiamondWorkflowName = "SimpleDiamondWorkflow"

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

	startedAt := workflow.Now(ctx)
	var prepare activities.WaitResult
	if err := scheduleWaitActivity(ctx, "prepare", prepareDuration).Get(ctx, &prepare); err != nil {
		return nil, err
	}
	branchAFuture := scheduleWaitActivity(ctx, "branch-a", branchADuration)
	branchBFuture := scheduleWaitActivity(ctx, "branch-b", branchBDuration)
	var branchA activities.WaitResult
	if err := branchAFuture.Get(ctx, &branchA); err != nil {
		return nil, err
	}
	var branchB activities.WaitResult
	if err := branchBFuture.Get(ctx, &branchB); err != nil {
		return nil, err
	}
	var finalize activities.WaitResult
	if err := scheduleWaitActivity(ctx, "finalize", finalizeDuration).Get(ctx, &finalize); err != nil {
		return nil, err
	}
	finishedAt := workflow.Now(ctx)

	return &orchestrationv1.SimpleDiamondResult{
		StartedAt:  timestamppb.New(startedAt),
		FinishedAt: timestamppb.New(finishedAt),
		Elapsed:    durationpb.New(finishedAt.Sub(startedAt)),
		Nodes:      waitResults([]activities.WaitResult{prepare, branchA, branchB, finalize}),
	}, nil
}
