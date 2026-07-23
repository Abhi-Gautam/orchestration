package activities

import "testing"

func TestSelectFaultModeIsReproducible(t *testing.T) {
	input := FaultActivityInput{
		Seed: 42,
		Probabilities: &OutcomeProbabilities{
			Success:             50,
			RetryableFailure:    20,
			NonRetryableFailure: 10,
			Panic:               10,
			StartToCloseTimeout: 5,
			HeartbeatTimeout:    5,
		},
	}

	first, err := selectFaultMode(input, "branch-0001", 1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := selectFaultMode(input, "branch-0001", 1)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("same seed, Activity ID, and attempt selected %q then %q", first, second)
	}
}

func TestSelectFaultModeRejectsInvalidProbabilityTotal(t *testing.T) {
	_, err := selectFaultMode(FaultActivityInput{
		Probabilities: &OutcomeProbabilities{Success: 99},
	}, "branch-0001", 1)
	if err == nil {
		t.Fatal("expected invalid probability total to fail")
	}
}
