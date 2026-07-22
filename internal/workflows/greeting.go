package workflows

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"orchestration/internal/activities"
)

const GreetingWorkflowName = "GreetingWorkflow"

func GreetingWorkflow(ctx workflow.Context, name string) (string, error) {
	options := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, options)

	var greeting string
	if err := workflow.ExecuteActivity(ctx, activities.FormatGreeting, name).Get(ctx, &greeting); err != nil {
		return "", err
	}

	return greeting, nil
}
