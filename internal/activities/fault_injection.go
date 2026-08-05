package activities

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/rand"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

type FaultMode string

const (
	InjectedRetryableFailureErrorType = "InjectedRetryableFailure"

	FaultSuccess             FaultMode = "success"
	FaultRetryableFailure    FaultMode = "retryable-failure"
	FaultNonRetryableFailure FaultMode = "non-retryable-failure"
	FaultPanic               FaultMode = "panic"
	FaultStartToCloseTimeout FaultMode = "start-to-close-timeout"
	FaultHeartbeatTimeout    FaultMode = "heartbeat-timeout"
	FaultWaitForCancellation FaultMode = "wait-for-cancellation"
)

type OutcomeProbabilities struct {
	Success             int
	RetryableFailure    int
	NonRetryableFailure int
	Panic               int
	StartToCloseTimeout int
	HeartbeatTimeout    int
}

type FaultActivityInput struct {
	Name              string
	WorkDuration      time.Duration
	StallDuration     time.Duration
	Mode              FaultMode
	Seed              int64
	Probabilities     *OutcomeProbabilities
	FailUntilAttempt  int32
	HeartbeatInterval time.Duration
}

type FaultActivityResult struct {
	Name       string
	Outcome    FaultMode
	Attempt    int32
	StartedAt  time.Time
	FinishedAt time.Time
	Elapsed    time.Duration
}

func FaultInjectionActivity(ctx context.Context, input FaultActivityInput) (FaultActivityResult, error) {
	info := activity.GetInfo(ctx)
	startedAt := time.Now().UTC()
	mode, err := selectFaultMode(input, info.ActivityID, info.Attempt)
	if err != nil {
		return FaultActivityResult{}, temporal.NewNonRetryableApplicationError(
			"invalid fault-injection configuration",
			"InvalidFaultConfiguration",
			err,
		)
	}
	if mode == FaultRetryableFailure && input.FailUntilAttempt > 0 && info.Attempt > input.FailUntilAttempt {
		mode = FaultSuccess
	}

	logger := activity.GetLogger(ctx)
	logger.Info("FaultInjectionActivity started",
		"name", input.Name,
		"mode", mode,
		"attempt", info.Attempt,
		"activityId", info.ActivityID,
	)

	result := func(outcome FaultMode) FaultActivityResult {
		finishedAt := time.Now().UTC()
		return FaultActivityResult{
			Name:       input.Name,
			Outcome:    outcome,
			Attempt:    info.Attempt,
			StartedAt:  startedAt,
			FinishedAt: finishedAt,
			Elapsed:    finishedAt.Sub(startedAt),
		}
	}

	switch mode {
	case FaultSuccess:
		if err := performCancellableWork(ctx, input.WorkDuration, input.HeartbeatInterval); err != nil {
			return FaultActivityResult{}, temporal.NewCanceledError(input.Name, info.Attempt)
		}
		return result(FaultSuccess), nil

	case FaultRetryableFailure:
		if err := performCancellableWork(ctx, input.WorkDuration, input.HeartbeatInterval); err != nil {
			return FaultActivityResult{}, temporal.NewCanceledError(input.Name, info.Attempt)
		}
		return FaultActivityResult{}, temporal.NewApplicationError(
			"injected retryable failure",
			InjectedRetryableFailureErrorType,
			input.Name,
			info.Attempt,
		)

	case FaultNonRetryableFailure:
		if err := performCancellableWork(ctx, input.WorkDuration, input.HeartbeatInterval); err != nil {
			return FaultActivityResult{}, temporal.NewCanceledError(input.Name, info.Attempt)
		}
		return FaultActivityResult{}, temporal.NewNonRetryableApplicationError(
			"injected non-retryable failure",
			"InjectedNonRetryableFailure",
			nil,
			input.Name,
			info.Attempt,
		)

	case FaultPanic:
		if err := performCancellableWork(ctx, input.WorkDuration, input.HeartbeatInterval); err != nil {
			return FaultActivityResult{}, temporal.NewCanceledError(input.Name, info.Attempt)
		}
		panic(fmt.Sprintf("injected panic in %s on attempt %d", input.Name, info.Attempt))

	case FaultStartToCloseTimeout:
		if err := performCancellableWork(ctx, stallDuration(input), input.HeartbeatInterval); err != nil {
			return FaultActivityResult{}, temporal.NewCanceledError(input.Name, info.Attempt)
		}
		return result(FaultStartToCloseTimeout), nil

	case FaultHeartbeatTimeout:
		activity.RecordHeartbeat(ctx, "heartbeat-before-stall", info.Attempt)
		time.Sleep(stallDuration(input))
		return result(FaultHeartbeatTimeout), nil

	case FaultWaitForCancellation:
		interval := input.HeartbeatInterval
		if interval <= 0 {
			interval = 100 * time.Millisecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				activity.RecordHeartbeat(ctx, "waiting-for-cancellation", info.Attempt)
			case <-ctx.Done():
				return FaultActivityResult{}, temporal.NewCanceledError(input.Name, info.Attempt)
			}
		}

	default:
		return FaultActivityResult{}, temporal.NewNonRetryableApplicationError(
			"unsupported fault mode",
			"InvalidFaultMode",
			nil,
			string(mode),
		)
	}
}

func selectFaultMode(input FaultActivityInput, activityID string, attempt int32) (FaultMode, error) {
	if input.Probabilities == nil {
		if input.Mode == "" {
			return FaultSuccess, nil
		}
		return input.Mode, nil
	}

	p := input.Probabilities
	weights := []struct {
		mode   FaultMode
		weight int
	}{
		{FaultSuccess, p.Success},
		{FaultRetryableFailure, p.RetryableFailure},
		{FaultNonRetryableFailure, p.NonRetryableFailure},
		{FaultPanic, p.Panic},
		{FaultStartToCloseTimeout, p.StartToCloseTimeout},
		{FaultHeartbeatTimeout, p.HeartbeatTimeout},
	}

	total := 0
	for _, candidate := range weights {
		if candidate.weight < 0 {
			return "", fmt.Errorf("probability for %s cannot be negative", candidate.mode)
		}
		total += candidate.weight
	}
	if total != 100 {
		return "", fmt.Errorf("probabilities must total 100, got %d", total)
	}

	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(activityID))
	seed := input.Seed + int64(hasher.Sum64()) + int64(attempt)
	roll := rand.New(rand.NewSource(seed)).Intn(100)
	cumulative := 0
	for _, candidate := range weights {
		cumulative += candidate.weight
		if roll < cumulative {
			return candidate.mode, nil
		}
	}
	return "", fmt.Errorf("no outcome selected for roll %d", roll)
}

func performCancellableWork(ctx context.Context, duration, heartbeatInterval time.Duration) error {
	if duration <= 0 {
		return nil
	}

	timer := time.NewTimer(duration)
	defer timer.Stop()
	if heartbeatInterval <= 0 {
		select {
		case <-timer.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-timer.C:
			return nil
		case <-ticker.C:
			activity.RecordHeartbeat(ctx, "working")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func stallDuration(input FaultActivityInput) time.Duration {
	if input.StallDuration > 0 {
		return input.StallDuration
	}
	return input.WorkDuration
}
