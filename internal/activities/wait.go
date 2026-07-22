package activities

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
)

type WaitInput struct {
	Name     string
	Duration time.Duration
}

type WaitResult struct {
	Name       string
	Attempt    int32
	StartedAt  time.Time
	FinishedAt time.Time
	Elapsed    time.Duration
}

func WaitActivity(ctx context.Context, input WaitInput) (WaitResult, error) {
	info := activity.GetInfo(ctx)
	logger := activity.GetLogger(ctx)
	startedAt := time.Now().UTC()

	logger.Info("WaitActivity started",
		"name", input.Name,
		"activityId", info.ActivityID,
		"attempt", info.Attempt,
		"startedAt", startedAt,
		"duration", input.Duration,
	)

	timer := time.NewTimer(input.Duration)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-ctx.Done():
		logger.Info("WaitActivity canceled",
			"name", input.Name,
			"activityId", info.ActivityID,
			"attempt", info.Attempt,
			"elapsed", time.Since(startedAt),
		)
		return WaitResult{}, ctx.Err()
	}

	finishedAt := time.Now().UTC()
	result := WaitResult{
		Name:       input.Name,
		Attempt:    info.Attempt,
		StartedAt:  startedAt,
		FinishedAt: finishedAt,
		Elapsed:    finishedAt.Sub(startedAt),
	}

	logger.Info("WaitActivity finished",
		"name", result.Name,
		"activityId", info.ActivityID,
		"attempt", result.Attempt,
		"startedAt", result.StartedAt,
		"finishedAt", result.FinishedAt,
		"elapsed", result.Elapsed,
	)

	return result, nil
}
