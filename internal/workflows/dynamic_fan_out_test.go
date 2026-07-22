package workflows_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"orchestration/internal/activities"
	"orchestration/internal/workflows"
)

func TestDynamicFanOutWorkflow(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivity(activities.PlanFanOutActivity)
	env.RegisterActivity(activities.WaitActivity)

	env.ExecuteWorkflow(workflows.DynamicFanOutWorkflow, workflows.DynamicFanOutInput{
		RequestedCount:   25,
		BranchDuration:   10 * time.Millisecond,
		FinalizeDuration: 5 * time.Millisecond,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result workflows.DynamicFanOutResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 25, result.PlannedCount)
	require.Equal(t, 25, result.CompletedCount)
	require.Greater(t, result.PeakConcurrentBranches, 1)
	require.False(t, result.Finalize.StartedAt.Before(result.LastBranchFinishedAt))
}
