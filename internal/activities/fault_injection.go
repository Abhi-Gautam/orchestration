package activities

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	orchestrationv1 "orchestration/gen/orchestration/v1"
)

const (
	InjectedRetryableFailureErrorType = "InjectedRetryableFailure"

	// The Workflow sets heartbeat deadlines well above this, so the interval only has to
	// keep cancellation observable. The SDK throttles heartbeats to 80% of the deadline.
	faultHeartbeatInterval = time.Second
)

// FaultActivityInput is a complete instruction. The Workflow resolved the branch's mode
// before scheduling so it could match the Activity's deadlines to the injected fault.
type FaultActivityInput struct {
	Name             string
	Mode             orchestrationv1.FaultMode
	WorkDuration     time.Duration
	StallDuration    time.Duration
	FailUntilAttempt int32
}

type FaultActivityResult struct {
	Name       string
	Outcome    orchestrationv1.FaultMode
	Attempt    int32
	StartedAt  time.Time
	FinishedAt time.Time
	Elapsed    time.Duration
}

func FaultInjectionActivity(ctx context.Context, input FaultActivityInput) (FaultActivityResult, error) {
	info := activity.GetInfo(ctx)
	startedAt := time.Now().UTC()

	// fail_until_attempt 0 never recovers; N fails attempts 1..N and succeeds afterwards.
	mode := input.Mode
	if mode == orchestrationv1.FaultMode_FAULT_MODE_RETRYABLE_FAILURE &&
		input.FailUntilAttempt > 0 && info.Attempt > input.FailUntilAttempt {
		mode = orchestrationv1.FaultMode_FAULT_MODE_SUCCESS
	}

	activity.GetLogger(ctx).Info("FaultInjectionActivity started",
		"name", input.Name,
		"mode", mode,
		"attempt", info.Attempt,
		"activityId", info.ActivityID,
	)

	result := func() (FaultActivityResult, error) {
		finishedAt := time.Now().UTC()
		return FaultActivityResult{
			Name:       input.Name,
			Outcome:    mode,
			Attempt:    info.Attempt,
			StartedAt:  startedAt,
			FinishedAt: finishedAt,
			Elapsed:    finishedAt.Sub(startedAt),
		}, nil
	}
	canceled := func() (FaultActivityResult, error) {
		return FaultActivityResult{}, temporal.NewCanceledError(input.Name, info.Attempt)
	}

	switch mode {
	case orchestrationv1.FaultMode_FAULT_MODE_SUCCESS:
		if work(ctx, input.WorkDuration) != nil {
			return canceled()
		}
		return result()

	case orchestrationv1.FaultMode_FAULT_MODE_RETRYABLE_FAILURE:
		if work(ctx, input.WorkDuration) != nil {
			return canceled()
		}
		return FaultActivityResult{}, temporal.NewApplicationError(
			"injected retryable failure",
			InjectedRetryableFailureErrorType,
			input.Name,
			info.Attempt,
		)

	case orchestrationv1.FaultMode_FAULT_MODE_NON_RETRYABLE_FAILURE:
		if work(ctx, input.WorkDuration) != nil {
			return canceled()
		}
		return FaultActivityResult{}, temporal.NewNonRetryableApplicationError(
			"injected non-retryable failure",
			"InjectedNonRetryableFailure",
			nil,
			input.Name,
			info.Attempt,
		)

	case orchestrationv1.FaultMode_FAULT_MODE_PANIC:
		if work(ctx, input.WorkDuration) != nil {
			return canceled()
		}
		panic(fmt.Sprintf("injected panic in %s on attempt %d", input.Name, info.Attempt))

	case orchestrationv1.FaultMode_FAULT_MODE_START_TO_CLOSE_TIMEOUT,
		orchestrationv1.FaultMode_FAULT_MODE_HEARTBEAT_TIMEOUT:
		// Both deadlines are breached the same way: stop heartbeating and outlive them.
		// Temporal decides the outcome; a result here means the deadline never fired.
		stall(input.StallDuration)
		return result()

	case orchestrationv1.FaultMode_FAULT_MODE_WAIT_FOR_CANCELLATION:
		<-ctx.Done()
		return canceled()

	default:
		return FaultActivityResult{}, temporal.NewNonRetryableApplicationError(
			"unsupported fault mode",
			"InvalidFaultMode",
			nil,
			mode.String(),
		)
	}
}

// work occupies the Activity while heartbeating, returning early once it is canceled.
func work(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	ticker := time.NewTicker(faultHeartbeatInterval)
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

// stall ignores cancellation on purpose. Returning as soon as the deadline cancels the
// context would race Temporal's own timeout, and when the Activity's response won that
// race the same injected fault was recorded as a cancellation, which also ended the
// retry chain early. Letting the deadline pass unanswered keeps Temporal authoritative.
func stall(duration time.Duration) {
	if duration > 0 {
		time.Sleep(duration)
	}
}
