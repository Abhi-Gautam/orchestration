package workflows

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	orchestrationv1 "orchestration/gen/orchestration/v1"
)

const (
	maxFanOutActivities = 1000

	fanOutWorkDuration      = 300 * time.Millisecond
	fanOutHeartbeatInterval = 100 * time.Millisecond
	fanOutStallDuration     = 3 * time.Second
)

func resolveFaultBranches(input *orchestrationv1.FanOutPolicyRequest) ([]*orchestrationv1.FaultBranchSpec, orchestrationv1.FaultCampaignType, int64, error) {
	if input == nil {
		return nil, orchestrationv1.FaultCampaignType_FAULT_CAMPAIGN_TYPE_UNSPECIFIED, 0, errors.New("input is required")
	}
	if err := validateAggregationPolicy(input.Policy); err != nil {
		return nil, orchestrationv1.FaultCampaignType_FAULT_CAMPAIGN_TYPE_UNSPECIFIED, 0, err
	}

	hasBranches := len(input.Branches) > 0
	hasCampaign := input.Campaign != nil
	if hasBranches == hasCampaign {
		return nil, orchestrationv1.FaultCampaignType_FAULT_CAMPAIGN_TYPE_UNSPECIFIED, 0, errors.New("exactly one of branches or campaign is required")
	}
	if hasBranches {
		if err := validateExplicitBranches(input.Branches); err != nil {
			return nil, orchestrationv1.FaultCampaignType_FAULT_CAMPAIGN_TYPE_UNSPECIFIED, 0, err
		}
		return input.Branches, orchestrationv1.FaultCampaignType_FAULT_CAMPAIGN_TYPE_UNSPECIFIED, 0, nil
	}

	branches, err := generateFaultCampaign(input.Campaign)
	if err != nil {
		return nil, orchestrationv1.FaultCampaignType_FAULT_CAMPAIGN_TYPE_UNSPECIFIED, 0, err
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

	activityIDs := make(map[string]struct{}, len(branches))
	for i, branch := range branches {
		if branch == nil || branch.Name == "" {
			return fmt.Errorf("branch %d requires a name", i)
		}
		if _, exists := activityIDs[branch.Name]; exists {
			return fmt.Errorf("branch %q has a duplicate name", branch.Name)
		}
		activityIDs[branch.Name] = struct{}{}

		if duration(branch.WorkDuration) < 0 || duration(branch.StallDuration) < 0 || duration(branch.HeartbeatInterval) < 0 {
			return fmt.Errorf("branch %q durations must not be negative", branch.Name)
		}
		if branch.Probabilities != nil {
			if branch.Mode != orchestrationv1.FaultMode_FAULT_MODE_UNSPECIFIED {
				return fmt.Errorf("branch %q cannot set both mode and probabilities", branch.Name)
			}
			if err := validateOutcomeProbabilities(branch.Probabilities); err != nil {
				return fmt.Errorf("branch %q: %w", branch.Name, err)
			}
			continue
		}
		if _, ok := activityFaultMode(branch.Mode); !ok {
			return fmt.Errorf("branch %q has an unsupported fault mode", branch.Name)
		}
	}
	return nil
}

func generateFaultCampaign(campaign *orchestrationv1.FaultCampaignSpec) ([]*orchestrationv1.FaultBranchSpec, error) {
	if campaign == nil {
		return nil, errors.New("campaign is required")
	}

	switch campaign.Type {
	case orchestrationv1.FaultCampaignType_FAULT_CAMPAIGN_TYPE_ALL_SUCCESS_V1:
		if campaign.ActivityCount < 1 || campaign.ActivityCount > maxFanOutActivities {
			return nil, fmt.Errorf("all-success activity_count must be between 1 and %d", maxFanOutActivities)
		}
		if campaign.BackgroundProbabilities != nil {
			return nil, errors.New("all-success campaign does not accept background_probabilities")
		}
		return generateAllSuccessCampaign(campaign), nil

	case orchestrationv1.FaultCampaignType_FAULT_CAMPAIGN_TYPE_MIXED_V1:
		if campaign.ActivityCount < 6 || campaign.ActivityCount > maxFanOutActivities {
			return nil, fmt.Errorf("mixed activity_count must be between 6 and %d", maxFanOutActivities)
		}
		probabilities := campaign.BackgroundProbabilities
		if probabilities == nil {
			probabilities = defaultMixedProbabilities()
		}
		if err := validateOutcomeProbabilities(probabilities); err != nil {
			return nil, err
		}
		return generateMixedCampaign(campaign, probabilities), nil

	default:
		return nil, errors.New("campaign type is required")
	}
}

func generateAllSuccessCampaign(campaign *orchestrationv1.FaultCampaignSpec) []*orchestrationv1.FaultBranchSpec {
	branches := make([]*orchestrationv1.FaultBranchSpec, campaign.ActivityCount)
	for i := range branches {
		branches[i] = baseGeneratedBranch(i, campaign.Seed)
		branches[i].Mode = orchestrationv1.FaultMode_FAULT_MODE_SUCCESS
	}
	return branches
}

func generateMixedCampaign(campaign *orchestrationv1.FaultCampaignSpec, probabilities *orchestrationv1.OutcomeProbabilities) []*orchestrationv1.FaultBranchSpec {
	branches := make([]*orchestrationv1.FaultBranchSpec, campaign.ActivityCount)
	for i := range branches {
		branches[i] = baseGeneratedBranch(i, campaign.Seed)
	}

	branches[0].Mode = orchestrationv1.FaultMode_FAULT_MODE_NON_RETRYABLE_FAILURE
	branches[0].WorkDuration = nil
	branches[1].Mode = orchestrationv1.FaultMode_FAULT_MODE_RETRYABLE_FAILURE
	branches[1].FailUntilAttempt = 1
	branches[1].WorkDuration = nil
	branches[2].Mode = orchestrationv1.FaultMode_FAULT_MODE_RETRYABLE_FAILURE
	branches[2].FailUntilAttempt = 3
	branches[2].WorkDuration = nil
	branches[3].Mode = orchestrationv1.FaultMode_FAULT_MODE_PANIC
	branches[3].WorkDuration = nil
	branches[4].Mode = orchestrationv1.FaultMode_FAULT_MODE_START_TO_CLOSE_TIMEOUT
	branches[5].Mode = orchestrationv1.FaultMode_FAULT_MODE_HEARTBEAT_TIMEOUT

	for i := 6; i < len(branches); i++ {
		branches[i].Mode = orchestrationv1.FaultMode_FAULT_MODE_UNSPECIFIED
		branches[i].FailUntilAttempt = 1
		branches[i].Probabilities = probabilities
	}
	return branches
}

func baseGeneratedBranch(index int, seed int64) *orchestrationv1.FaultBranchSpec {
	return &orchestrationv1.FaultBranchSpec{
		Name:              fmt.Sprintf("fault-%06d", index),
		Seed:              seed,
		WorkDuration:      durationpb.New(fanOutWorkDuration),
		StallDuration:     durationpb.New(fanOutStallDuration),
		HeartbeatInterval: durationpb.New(fanOutHeartbeatInterval),
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
	weights := []struct {
		name  string
		value int32
	}{
		{"success", probabilities.Success},
		{"retryable_failure", probabilities.RetryableFailure},
		{"non_retryable_failure", probabilities.NonRetryableFailure},
		{"panic", probabilities.Panic},
		{"start_to_close_timeout", probabilities.StartToCloseTimeout},
		{"heartbeat_timeout", probabilities.HeartbeatTimeout},
	}

	var total int32
	for _, weight := range weights {
		if weight.value < 0 {
			return fmt.Errorf("probability %s cannot be negative", weight.name)
		}
		total += weight.value
	}
	if total != 100 {
		return fmt.Errorf("background probabilities must total 100, got %d", total)
	}
	return nil
}
