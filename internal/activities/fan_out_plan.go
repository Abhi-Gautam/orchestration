package activities

import (
	"context"

	"go.temporal.io/sdk/activity"
)

type FanOutPlanInput struct {
	Count int
}

type FanOutPlanResult struct {
	Count int
}

func PlanFanOutActivity(ctx context.Context, input FanOutPlanInput) (FanOutPlanResult, error) {
	activity.GetLogger(ctx).Info("fan-out plan created", "count", input.Count)
	return FanOutPlanResult{Count: input.Count}, nil
}
