package activities

import (
	"fmt"
	"testing"
)

func TestSelectFaultModeIsDeterministic(t *testing.T) {
	input := FaultActivityInput{
		Seed: 17,
		Probabilities: &OutcomeProbabilities{
			Success: 82, RetryableFailure: 8, NonRetryableFailure: 3,
			Panic: 2, StartToCloseTimeout: 3, HeartbeatTimeout: 2,
		},
	}
	first, err := selectFaultMode(input, "fault-000042", 2)
	if err != nil {
		t.Fatalf("select first mode: %v", err)
	}
	second, err := selectFaultMode(input, "fault-000042", 2)
	if err != nil {
		t.Fatalf("select second mode: %v", err)
	}
	if first != second {
		t.Fatalf("modes differ for identical inputs: %q and %q", first, second)
	}
}

func TestSelectFaultModeAllSuccessProfile(t *testing.T) {
	input := FaultActivityInput{Seed: 23, Probabilities: &OutcomeProbabilities{Success: 100}}
	for attempt := int32(1); attempt <= 3; attempt++ {
		for index := 0; index < 1000; index++ {
			mode, err := selectFaultMode(input, fmt.Sprintf("fault-%06d", index), attempt)
			if err != nil {
				t.Fatalf("select mode: %v", err)
			}
			if mode != FaultSuccess {
				t.Fatalf("activity %d attempt %d mode = %q, want success", index, attempt, mode)
			}
		}
	}
}

func TestSelectFaultModeRejectsInvalidProbabilities(t *testing.T) {
	tests := []OutcomeProbabilities{
		{Success: 99},
		{Success: 101},
		{Success: 100, Panic: -1, RetryableFailure: 1},
	}
	for _, probabilities := range tests {
		_, err := selectFaultMode(FaultActivityInput{Probabilities: &probabilities}, "fault-000000", 1)
		if err == nil {
			t.Fatalf("probabilities %+v unexpectedly accepted", probabilities)
		}
	}
}
