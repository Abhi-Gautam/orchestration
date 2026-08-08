package activities

import (
	"context"

	"go.temporal.io/sdk/activity"

	orchestrationv1 "orchestration/gen/orchestration/v1"
)

// Attempt is the single detail every Activity attaches to a heartbeat or failure. Building
// it in one place keeps the Workflow's decoder and the Activity's encoder in one contract.
func Attempt(ctx context.Context) *orchestrationv1.ActivityAttempt {
	info := activity.GetInfo(ctx)
	return &orchestrationv1.ActivityAttempt{ActivityId: info.ActivityID, Attempt: info.Attempt}
}
