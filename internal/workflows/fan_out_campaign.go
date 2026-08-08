package workflows

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	orchestrationv1 "orchestration/gen/orchestration/v1"
)

const (
	maxFanOutActivities = 1000

	fanOutWorkDuration  = 300 * time.Millisecond
	fanOutStallDuration = 3 * time.Second
)

type outcomeWeight struct {
	name   string
	mode   orchestrationv1.FaultMode
	weight int32
}

func resolveFaultBranches(input *orchestrationv1.FanOutPolicyRequest) ([]*orchestrationv1.FaultBranchSpec, orchestrationv1.FaultCampaignType, int64, error) {
	unspecified := orchestrationv1.FaultCampaignType_FAULT_CAMPAIGN_TYPE_UNSPECIFIED
	if input == nil {
		return nil, unspecified, 0, errors.New("input is required")
	}
	if err := validateAggregationPolicy(input.Policy); err != nil {
		return nil, unspecified, 0, err
	}
	if (len(input.Branches) > 0) == (input.Campaign != nil) {
		return nil, unspecified, 0, errors.New("exactly one of branches or campaign is required")
	}

	if len(input.Branches) > 0 {
		if err := validateExplicitBranches(input.Branches); err != nil {
			return nil, unspecified, 0, err
		}
		return input.Branches, unspecified, 0, nil
	}

	branches, err := generateFaultCampaign(input.Campaign)
	if err != nil {
		return nil, unspecified, 0, err
	}
	return branches, input.Campaign.Type, input.Campaign.Seed, nil
}

func validateAggregationPolicy(policy orchestrationv1.AggregationPolicy) error {
	switch policy {
	case orchestrationv1.AggregationPolicy_AGGREGATION_POLICY_FAIL_FAST,
		orchestrationv1.AggregationPolicy_AGGREGATION_POLICY_ALL_SETTLED,
		orchestrationv1.AggregationPolicy_AGGREGATION_POLICY_ALL_SETTLED_THEN_FAIL:
		return nil
	default:
		return errors.New("policy is required")
	}
}

func validateExplicitBranches(branches []*orchestrationv1.FaultBranchSpec) error {
	if len(branches) > maxFanOutActivities {
		return fmt.Errorf("branches cannot exceed %d", maxFanOutActivities)
	}

	names := make(map[string]struct{}, len(branches))
	for index, branch := range branches {
		if branch == nil || branch.Name == "" {
			return fmt.Errorf("branch %d requires a name", index)
		}
		if _, duplicate := names[branch.Name]; duplicate {
			return fmt.Errorf("branch %q has a duplicate name", branch.Name)
		}
		names[branch.Name] = struct{}{}

		if !validFaultMode(branch.Mode) {
			return fmt.Errorf("branch %q requires a supported fault mode", branch.Name)
		}
		if duration(branch.WorkDuration) < 0 || duration(branch.StallDuration) < 0 {
			return fmt.Errorf("branch %q durations must not be negative", branch.Name)
		}
	}
	return nil
}

func generateFaultCampaign(campaign *orchestrationv1.FaultCampaignSpec) ([]*orchestrationv1.FaultBranchSpec, error) {
	switch campaign.Type {
	case orchestrationv1.FaultCampaignType_FAULT_CAMPAIGN_TYPE_ALL_SUCCESS_V1:
		if campaign.ActivityCount < 1 || campaign.ActivityCount > maxFanOutActivities {
			return nil, fmt.Errorf("all-success activity_count must be between 1 and %d", maxFanOutActivities)
		}
		if campaign.BackgroundProbabilities != nil {
			return nil, errors.New("all-success campaign does not accept background_probabilities")
		}
		branches := make([]*orchestrationv1.FaultBranchSpec, campaign.ActivityCount)
		for index := range branches {
			branches[index] = generatedBranch(index, orchestrationv1.FaultMode_FAULT_MODE_SUCCESS)
		}
		return branches, nil

	case orchestrationv1.FaultCampaignType_FAULT_CAMPAIGN_TYPE_MIXED_V1:
		if campaign.ActivityCount < int32(len(guaranteedMixedModes())) || campaign.ActivityCount > maxFanOutActivities {
			return nil, fmt.Errorf("mixed activity_count must be between %d and %d", len(guaranteedMixedModes()), maxFanOutActivities)
		}
		probabilities := campaign.BackgroundProbabilities
		if probabilities == nil {
			probabilities = defaultMixedProbabilities()
		}
		if err := validateOutcomeProbabilities(probabilities); err != nil {
			return nil, err
		}
		return generateMixedCampaign(campaign.ActivityCount, campaign.Seed, probabilities), nil

	default:
		return nil, errors.New("campaign type is required")
	}
}

// guaranteedMixedModes makes every classification the failure breakdown reports observable
// in a single run, instead of relying on the background roll to produce each rare case.
func guaranteedMixedModes() []orchestrationv1.FaultMode {
	return []orchestrationv1.FaultMode{
		orchestrationv1.FaultMode_FAULT_MODE_NON_RETRYABLE_FAILURE,
		orchestrationv1.FaultMode_FAULT_MODE_RETRYABLE_FAILURE,
		orchestrationv1.FaultMode_FAULT_MODE_RETRYABLE_FAILURE,
		orchestrationv1.FaultMode_FAULT_MODE_PANIC,
		orchestrationv1.FaultMode_FAULT_MODE_START_TO_CLOSE_TIMEOUT,
		orchestrationv1.FaultMode_FAULT_MODE_HEARTBEAT_TIMEOUT,
	}
}

func generateMixedCampaign(count int32, seed int64, probabilities *orchestrationv1.OutcomeProbabilities) []*orchestrationv1.FaultBranchSpec {
	guaranteed := guaranteedMixedModes()
	branches := make([]*orchestrationv1.FaultBranchSpec, count)
	for index := range branches {
		if index < len(guaranteed) {
			branches[index] = generatedBranch(index, guaranteed[index])
			continue
		}
		branches[index] = generatedBranch(index, rollFaultMode(seed, index, probabilities))
	}

	// Branch 1 recovers within its retry budget; branch 2 exhausts it.
	branches[1].FailUntilAttempt = 1
	branches[2].FailUntilAttempt = fanOutMaximumAttempts
	for index := len(guaranteed); index < len(branches); index++ {
		branches[index].FailUntilAttempt = 1
	}
	return branches
}

func generatedBranch(index int, mode orchestrationv1.FaultMode) *orchestrationv1.FaultBranchSpec {
	branch := &orchestrationv1.FaultBranchSpec{
		Name:         fmt.Sprintf("fault-%06d", index),
		Mode:         mode,
		WorkDuration: durationpb.New(fanOutWorkDuration),
	}
	if mode == orchestrationv1.FaultMode_FAULT_MODE_START_TO_CLOSE_TIMEOUT ||
		mode == orchestrationv1.FaultMode_FAULT_MODE_HEARTBEAT_TIMEOUT {
		branch.WorkDuration = nil
		branch.StallDuration = durationpb.New(fanOutStallDuration)
	}
	return branch
}

// rollFaultMode hashes the whole (seed, index) tuple so the same campaign seed always
// produces the same plan. Summing per-branch seeds would let distinct branches collide.
func rollFaultMode(seed int64, index int, probabilities *orchestrationv1.OutcomeProbabilities) orchestrationv1.FaultMode {
	var buffer [16]byte
	binary.BigEndian.PutUint64(buffer[0:8], uint64(seed))
	binary.BigEndian.PutUint64(buffer[8:16], uint64(index))
	hasher := fnv.New64a()
	_, _ = hasher.Write(buffer[:])

	roll := int32(hasher.Sum64() % 100)
	cumulative := int32(0)
	for _, candidate := range outcomeWeights(probabilities) {
		cumulative += candidate.weight
		if roll < cumulative {
			return candidate.mode
		}
	}
	return orchestrationv1.FaultMode_FAULT_MODE_SUCCESS
}

func outcomeWeights(probabilities *orchestrationv1.OutcomeProbabilities) []outcomeWeight {
	return []outcomeWeight{
		{"success", orchestrationv1.FaultMode_FAULT_MODE_SUCCESS, probabilities.Success},
		{"retryable_failure", orchestrationv1.FaultMode_FAULT_MODE_RETRYABLE_FAILURE, probabilities.RetryableFailure},
		{"non_retryable_failure", orchestrationv1.FaultMode_FAULT_MODE_NON_RETRYABLE_FAILURE, probabilities.NonRetryableFailure},
		{"panic", orchestrationv1.FaultMode_FAULT_MODE_PANIC, probabilities.Panic},
		{"start_to_close_timeout", orchestrationv1.FaultMode_FAULT_MODE_START_TO_CLOSE_TIMEOUT, probabilities.StartToCloseTimeout},
		{"heartbeat_timeout", orchestrationv1.FaultMode_FAULT_MODE_HEARTBEAT_TIMEOUT, probabilities.HeartbeatTimeout},
	}
}

func defaultMixedProbabilities() *orchestrationv1.OutcomeProbabilities {
	return &orchestrationv1.OutcomeProbabilities{
		Success:             82,
		RetryableFailure:    8,
		NonRetryableFailure: 3,
		Panic:               2,
		StartToCloseTimeout: 3,
		HeartbeatTimeout:    2,
	}
}

func validateOutcomeProbabilities(probabilities *orchestrationv1.OutcomeProbabilities) error {
	total := int32(0)
	for _, candidate := range outcomeWeights(probabilities) {
		if candidate.weight < 0 {
			return fmt.Errorf("probability %s cannot be negative", candidate.name)
		}
		total += candidate.weight
	}
	if total != 100 {
		return fmt.Errorf("background probabilities must total 100, got %d", total)
	}
	return nil
}

func validFaultMode(mode orchestrationv1.FaultMode) bool {
	switch mode {
	case orchestrationv1.FaultMode_FAULT_MODE_SUCCESS,
		orchestrationv1.FaultMode_FAULT_MODE_RETRYABLE_FAILURE,
		orchestrationv1.FaultMode_FAULT_MODE_NON_RETRYABLE_FAILURE,
		orchestrationv1.FaultMode_FAULT_MODE_PANIC,
		orchestrationv1.FaultMode_FAULT_MODE_START_TO_CLOSE_TIMEOUT,
		orchestrationv1.FaultMode_FAULT_MODE_HEARTBEAT_TIMEOUT,
		orchestrationv1.FaultMode_FAULT_MODE_WAIT_FOR_CANCELLATION:
		return true
	default:
		return false
	}
}
