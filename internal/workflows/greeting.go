package workflows

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	orchestrationv1 "orchestration/gen/orchestration/v1"
	"orchestration/internal/activities"
)

func GreetingWorkflow(ctx workflow.Context, input *orchestrationv1.GreetingRequest) (*orchestrationv1.GreetingResult, error) {
	if input == nil || input.Name == "" {
		return nil, invalidRequest("INVALID_GREETING_REQUEST", "name is required")
	}

	status, err := newStatusTracker(ctx, 1, "greeting", "format-greeting", "Formatting greeting")
	if err != nil {
		return nil, err
	}

	status.scheduleWork(1)
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	})

	var greeting string
	if err := workflow.ExecuteActivity(ctx, activities.FormatGreeting, input.Name).Get(ctx, &greeting); err != nil {
		return nil, err
	}

	status.recordSucceeded()
	status.setSucceeded("completed", "Greeting completed")
	return &orchestrationv1.GreetingResult{Greeting: greeting}, nil
}
