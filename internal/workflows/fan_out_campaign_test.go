package workflows

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	orchestrationv1 "orchestration/gen/orchestration/v1"
	"orchestration/internal/activities"
)

func TestGenerateAllSuccessCampaign(t *testing.T) {
	campaign := &orchestrationv1.FaultCampaignSpec{
		Type:          orchestrationv1.FaultCampaignType_FAULT_CAMPAIGN_TYPE_ALL_SUCCESS_V1,
		ActivityCount: 1000,
		Seed:          42,
	}
	branches, err := generateFaultCampaign(campaign)
	if err != nil {
		t.Fatalf("generate campaign: %v", err)
	}
	if len(branches) != 1000 {
		t.Fatalf("branch count = %d, want 1000", len(branches))
	}
	for i, branch := range branches {
		if want := fmt.Sprintf("fault-%06d", i); branch.Name != want {
			t.Fatalf("branch %d ID = %q, want %q", i, branch.Name, want)
		}
		if branch.Mode != orchestrationv1.FaultMode_FAULT_MODE_SUCCESS {
			t.Fatalf("branch %d mode = %s, want success", i, branch.Mode)
		}
		if branch.Seed != campaign.Seed || branch.Probabilities != nil {
			t.Fatalf("branch %d does not preserve all-success configuration", i)
		}
	}
}

func TestGenerateMixedCampaignGuaranteesCoverage(t *testing.T) {
	campaign := &orchestrationv1.FaultCampaignSpec{
		Type:          orchestrationv1.FaultCampaignType_FAULT_CAMPAIGN_TYPE_MIXED_V1,
		ActivityCount: 1000,
		Seed:          99,
	}
	branches, err := generateFaultCampaign(campaign)
	if err != nil {
		t.Fatalf("generate campaign: %v", err)
	}

	wantModes := []orchestrationv1.FaultMode{
		orchestrationv1.FaultMode_FAULT_MODE_NON_RETRYABLE_FAILURE,
		orchestrationv1.FaultMode_FAULT_MODE_RETRYABLE_FAILURE,
		orchestrationv1.FaultMode_FAULT_MODE_RETRYABLE_FAILURE,
		orchestrationv1.FaultMode_FAULT_MODE_PANIC,
		orchestrationv1.FaultMode_FAULT_MODE_START_TO_CLOSE_TIMEOUT,
		orchestrationv1.FaultMode_FAULT_MODE_HEARTBEAT_TIMEOUT,
	}
	for i, want := range wantModes {
		if branches[i].Mode != want {
			t.Fatalf("coverage branch %d mode = %s, want %s", i, branches[i].Mode, want)
		}
	}
	if branches[1].FailUntilAttempt != 1 || branches[2].FailUntilAttempt != 3 {
		t.Fatalf("coverage retry attempts = (%d, %d), want (1, 3)", branches[1].FailUntilAttempt, branches[2].FailUntilAttempt)
	}

	background := branches[6]
	if background.Mode != orchestrationv1.FaultMode_FAULT_MODE_UNSPECIFIED || background.FailUntilAttempt != 1 {
		t.Fatalf("unexpected background branch: %v", background)
	}
	if background.Probabilities == nil || probabilityTotal(background.Probabilities) != 100 {
		t.Fatalf("invalid background probabilities: %v", background.Probabilities)
	}
	if background.Probabilities.Success != 82 || background.Probabilities.RetryableFailure != 8 {
		t.Fatalf("unexpected default probabilities: %v", background.Probabilities)
	}
}

func TestCampaignValidationRejectsAmbiguousAndInvalidInputs(t *testing.T) {
	tests := []struct {
		name    string
		request *orchestrationv1.FanOutPolicyRequest
		want    string
	}{
		{
			name:    "requires one workload",
			request: &orchestrationv1.FanOutPolicyRequest{Policy: orchestrationv1.AggregationPolicy_AGGREGATION_POLICY_ALL_SETTLED},
			want:    "exactly one",
		},
		{
			name: "rejects campaign and branches",
			request: &orchestrationv1.FanOutPolicyRequest{
				Policy:   orchestrationv1.AggregationPolicy_AGGREGATION_POLICY_ALL_SETTLED,
				Campaign: validMixedCampaign(),
				Branches: []*orchestrationv1.FaultBranchSpec{{Name: "branch", Mode: orchestrationv1.FaultMode_FAULT_MODE_SUCCESS}},
			},
			want: "exactly one",
		},
		{
			name: "rejects invalid probability total",
			request: &orchestrationv1.FanOutPolicyRequest{
				Policy: orchestrationv1.AggregationPolicy_AGGREGATION_POLICY_ALL_SETTLED,
				Campaign: &orchestrationv1.FaultCampaignSpec{
					Type:                    orchestrationv1.FaultCampaignType_FAULT_CAMPAIGN_TYPE_MIXED_V1,
					ActivityCount:           1000,
					BackgroundProbabilities: &orchestrationv1.OutcomeProbabilities{Success: 99},
				},
			},
			want: "total 100",
		},
		{
			name: "rejects overflowing total",
			request: &orchestrationv1.FanOutPolicyRequest{
				Policy: orchestrationv1.AggregationPolicy_AGGREGATION_POLICY_ALL_SETTLED,
				Campaign: &orchestrationv1.FaultCampaignSpec{
					Type:          orchestrationv1.FaultCampaignType_FAULT_CAMPAIGN_TYPE_MIXED_V1,
					ActivityCount: 1000,
					BackgroundProbabilities: &orchestrationv1.OutcomeProbabilities{
						Success: 100, RetryableFailure: 1<<31 - 1, NonRetryableFailure: 1,
					},
				},
			},
			want: "total 100",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, _, err := resolveFaultBranches(test.request)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestFanOutAggregateConsumesSuccessfulOutputsInInputOrder(t *testing.T) {
	collector := &fanOutCollector{
		result:         &orchestrationv1.FanOutPolicyResult{FailureBreakdown: &orchestrationv1.FanOutFailureBreakdown{}},
		successDigests: make([][]byte, 3),
		sampleKeys:     make(map[string]struct{}),
	}
	results := []activities.FaultActivityResult{
		{Name: "result-a", Outcome: activities.FaultSuccess, Attempt: 1},
		{Name: "result-b", Outcome: activities.FaultSuccess, Attempt: 2},
		{Name: "result-c", Outcome: activities.FaultSuccess, Attempt: 1},
	}
	activityIDs := []string{"fault-000000", "fault-000001", "fault-000002"}
	for _, index := range []int{2, 0, 1} {
		outcome := &orchestrationv1.ActivityOutcome{ActivityId: activityIDs[index], Index: int32(index)}
		collector.recordSuccess(index, outcome, results[index])
	}

	aggregate := collector.completeAggregate()
	if !aggregate.Complete || aggregate.ConsumedSuccessfulOutputs != 3 {
		t.Fatalf("aggregate = %v, want complete with 3 outputs", aggregate)
	}
	combined := sha256.New()
	for i, result := range results {
		_, _ = combined.Write(successfulResultDigest(i, activityIDs[i], result))
	}
	if want := hex.EncodeToString(combined.Sum(nil)); aggregate.SemanticDigest != want {
		t.Fatalf("digest = %q, want %q", aggregate.SemanticDigest, want)
	}
	if collector.result.SucceededFirstAttempt != 2 || collector.result.SucceededAfterRetry != 1 {
		t.Fatalf("success counts = first:%d retried:%d", collector.result.SucceededFirstAttempt, collector.result.SucceededAfterRetry)
	}
}

func TestFanOutResultRemainsCompact(t *testing.T) {
	result := &orchestrationv1.FanOutPolicyResult{
		Planned:          1000,
		Succeeded:        900,
		Failed:           99,
		Canceled:         1,
		Aggregate:        &orchestrationv1.FanOutAggregate{Complete: true, ConsumedSuccessfulOutputs: 900, SemanticDigest: strings.Repeat("a", 64)},
		FailureBreakdown: &orchestrationv1.FanOutFailureBreakdown{RetryExhausted: 10, NonRetryable: 30, Panic: 20, StartToCloseTimeout: 20, HeartbeatTimeout: 19, Canceled: 1},
	}
	for i := 0; i < maxFanOutSamples; i++ {
		result.Samples = append(result.Samples, &orchestrationv1.ActivityOutcome{
			ActivityId: fmt.Sprintf("fault-%06d", i),
			Index:      int32(i),
			Failure: &orchestrationv1.ActivityFailure{
				Kind:    orchestrationv1.ActivityFailureKind_ACTIVITY_FAILURE_KIND_UNKNOWN,
				Message: strings.Repeat("x", 512),
			},
		})
	}
	if size := proto.Size(result); size >= 64*1024 {
		t.Fatalf("serialized result size = %d bytes, want below 64 KiB", size)
	}
}

func TestSafeFailureMessageIsSingleLineAndBounded(t *testing.T) {
	got := safeFailureMessage(strings.Repeat("x", 600) + "\nstack trace")
	if len(got) != 512 || strings.Contains(got, "\n") {
		t.Fatalf("safe message has length %d and newline=%t", len(got), strings.Contains(got, "\n"))
	}
}

func validMixedCampaign() *orchestrationv1.FaultCampaignSpec {
	return &orchestrationv1.FaultCampaignSpec{
		Type:          orchestrationv1.FaultCampaignType_FAULT_CAMPAIGN_TYPE_MIXED_V1,
		ActivityCount: 1000,
		Seed:          1,
	}
}

func probabilityTotal(probabilities *orchestrationv1.OutcomeProbabilities) int64 {
	return int64(probabilities.Success) + int64(probabilities.RetryableFailure) + int64(probabilities.NonRetryableFailure) + int64(probabilities.Panic) + int64(probabilities.StartToCloseTimeout) + int64(probabilities.HeartbeatTimeout)
}
