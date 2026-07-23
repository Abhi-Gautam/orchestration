package workflows_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"orchestration/internal/activities"
	"orchestration/internal/workflows"
)

func TestFanOutAllSettledCollectsFailuresAndRetrySuccess(t *testing.T) {
	env := newFanOutTestEnvironment()
	branches := []workflows.FaultBranch{
		faultBranch("success", activities.FaultActivityInput{Mode: activities.FaultSuccess, WorkDuration: 10 * time.Millisecond}, 2),
		faultBranch("retry-then-success", activities.FaultActivityInput{Mode: activities.FaultRetryableFailure, FailUntilAttempt: 1}, 2),
		faultBranch("permanent-failure", activities.FaultActivityInput{Mode: activities.FaultNonRetryableFailure}, 2),
	}

	env.ExecuteWorkflow(workflows.FanOutPolicyWorkflow, workflows.FanOutPolicyInput{
		Policy:   workflows.AllSettled,
		Branches: branches,
	})

	require.NoError(t, env.GetWorkflowError())
	var result workflows.FanOutResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 3, result.Planned)
	require.Equal(t, 2, result.Succeeded)
	require.Equal(t, 1, result.Failed)
	require.Equal(t, int32(2), result.Outcomes[1].Result.Attempt)
	require.Equal(t, workflows.ApplicationFailure, result.Outcomes[2].Failure.Kind)
	require.True(t, result.Outcomes[2].Failure.NonRetryable)
}

func TestFanOutAllSettledThenFailReturnsAggregateError(t *testing.T) {
	env := newFanOutTestEnvironment()
	env.ExecuteWorkflow(workflows.FanOutPolicyWorkflow, workflows.FanOutPolicyInput{
		Policy: workflows.AllSettledThenFail,
		Branches: []workflows.FaultBranch{
			faultBranch("success", activities.FaultActivityInput{Mode: activities.FaultSuccess}, 1),
			faultBranch("failure", activities.FaultActivityInput{Mode: activities.FaultNonRetryableFailure}, 1),
		},
	})

	var applicationErr *temporal.ApplicationError
	require.ErrorAs(t, env.GetWorkflowError(), &applicationErr)
	require.Equal(t, "FanOutAggregateFailure", applicationErr.Type())
}

func TestFanOutFailFastCancelsSibling(t *testing.T) {
	env := newFanOutTestEnvironment()
	env.ExecuteWorkflow(workflows.FanOutPolicyWorkflow, workflows.FanOutPolicyInput{
		Policy: workflows.FailFast,
		Branches: []workflows.FaultBranch{
			faultBranch("failure", activities.FaultActivityInput{
				Mode:         activities.FaultNonRetryableFailure,
				WorkDuration: 20 * time.Millisecond,
			}, 1),
			faultBranch("waiting", activities.FaultActivityInput{
				Mode:              activities.FaultWaitForCancellation,
				HeartbeatInterval: 10 * time.Millisecond,
			}, 1),
		},
	})

	require.Error(t, env.GetWorkflowError())
}

func TestFanOutClassifiesPanic(t *testing.T) {
	env := newFanOutTestEnvironment()
	env.ExecuteWorkflow(workflows.FanOutPolicyWorkflow, workflows.FanOutPolicyInput{
		Policy: workflows.AllSettled,
		Branches: []workflows.FaultBranch{
			faultBranch("panic", activities.FaultActivityInput{Mode: activities.FaultPanic}, 1),
		},
	})

	require.NoError(t, env.GetWorkflowError())
	var result workflows.FanOutResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, workflows.PanicFailure, result.Outcomes[0].Failure.Kind)
}

func TestFanOutClassifiesHeartbeatTimeout(t *testing.T) {
	env := newFanOutTestEnvironment()
	branch := faultBranch("heartbeat-timeout", activities.FaultActivityInput{
		Mode:          activities.FaultHeartbeatTimeout,
		StallDuration: 1500 * time.Millisecond,
	}, 1)
	branch.StartToCloseTimeout = 3 * time.Second
	branch.ScheduleToCloseTimeout = 3 * time.Second
	branch.HeartbeatTimeout = 200 * time.Millisecond

	env.ExecuteWorkflow(workflows.FanOutPolicyWorkflow, workflows.FanOutPolicyInput{
		Policy:   workflows.AllSettled,
		Branches: []workflows.FaultBranch{branch},
	})

	require.NoError(t, env.GetWorkflowError())
	var result workflows.FanOutResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, workflows.TimeoutFailure, result.Outcomes[0].Failure.Kind)
	require.Equal(t, "Heartbeat", result.Outcomes[0].Failure.TimeoutType)
}

func TestFanOutClassifiesStartToCloseTimeout(t *testing.T) {
	env := newFanOutTestEnvironment()
	branch := faultBranch("timeout", activities.FaultActivityInput{
		Mode:          activities.FaultStartToCloseTimeout,
		StallDuration: 1500 * time.Millisecond,
	}, 1)
	branch.StartToCloseTimeout = 200 * time.Millisecond
	branch.ScheduleToCloseTimeout = 2 * time.Second

	env.ExecuteWorkflow(workflows.FanOutPolicyWorkflow, workflows.FanOutPolicyInput{
		Policy:   workflows.AllSettled,
		Branches: []workflows.FaultBranch{branch},
	})

	require.NoError(t, env.GetWorkflowError())
	var result workflows.FanOutResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, workflows.TimeoutFailure, result.Outcomes[0].Failure.Kind)
}

func newFanOutTestEnvironment() *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivity(activities.FaultInjectionActivity)
	env.RegisterActivity(activities.WaitActivity)
	return env
}

func faultBranch(id string, input activities.FaultActivityInput, maximumAttempts int32) workflows.FaultBranch {
	input.Name = id
	return workflows.FaultBranch{
		ActivityID:             id,
		Input:                  input,
		StartToCloseTimeout:    2 * time.Second,
		ScheduleToCloseTimeout: 5 * time.Second,
		HeartbeatTimeout:       500 * time.Millisecond,
		RetryPolicy: temporal.RetryPolicy{
			InitialInterval:    10 * time.Millisecond,
			BackoffCoefficient: 1,
			MaximumInterval:    10 * time.Millisecond,
			MaximumAttempts:    maximumAttempts,
		},
	}
}
