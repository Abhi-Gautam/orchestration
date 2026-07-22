package workflows_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"orchestration/internal/activities"
	"orchestration/internal/workflows"
)

func TestSimpleDiamondWorkflow(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivity(activities.WaitActivity)

	env.ExecuteWorkflow(workflows.SimpleDiamondWorkflow, workflows.SimpleDiamondInput{
		PrepareDuration:  10 * time.Millisecond,
		BranchADuration:  20 * time.Millisecond,
		BranchBDuration:  30 * time.Millisecond,
		FinalizeDuration: 10 * time.Millisecond,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result workflows.SimpleDiamondResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Len(t, result.Nodes, 4)
	require.Equal(t, []string{"prepare", "branch-a", "branch-b", "finalize"}, []string{
		result.Nodes[0].Name,
		result.Nodes[1].Name,
		result.Nodes[2].Name,
		result.Nodes[3].Name,
	})

	prepare, branchA, branchB, finalize := result.Nodes[0], result.Nodes[1], result.Nodes[2], result.Nodes[3]
	require.False(t, branchA.StartedAt.Before(prepare.FinishedAt))
	require.False(t, branchB.StartedAt.Before(prepare.FinishedAt))
	require.True(t, branchA.StartedAt.Before(branchB.FinishedAt))
	require.True(t, branchB.StartedAt.Before(branchA.FinishedAt))
	require.False(t, finalize.StartedAt.Before(branchA.FinishedAt))
	require.False(t, finalize.StartedAt.Before(branchB.FinishedAt))
}
