package workflows

import (
	"time"

	"go.temporal.io/sdk/workflow"

	"orchestration/internal/activities"
)

func scheduleWaitActivity(ctx workflow.Context, name string, duration time.Duration) workflow.Future {
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:          name,
		StartToCloseTimeout: duration + 10*time.Second,
	})
	return workflow.ExecuteActivity(activityCtx, activities.WaitActivity, activities.WaitInput{
		Name:     name,
		Duration: duration,
	})
}
