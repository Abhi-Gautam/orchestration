package workflowcatalog

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	orchestrationv1 "orchestration/gen/orchestration/v1"
)

const (
	GreetingWorkflowName          = "GreetingWorkflow"
	SimpleDiamondWorkflowName     = "SimpleDiamondWorkflow"
	DynamicFanOutWorkflowName     = "DynamicFanOutWorkflow"
	ReusableArtifactWorkflowName  = "ReusableArtifactWorkflow"
	DurableReportWorkflowName     = "DurableReportWorkflow"
	FanOutPolicyWorkflowName      = "FanOutPolicyWorkflow"
	ConditionalBranchWorkflowName = "ConditionalBranchWorkflow"
	OperationStatusQueryName      = "operation-status"

	ReusableArtifactCount         = 5
	ArtifactActivityTimeoutMargin = 30 * time.Second
)

type Definition struct {
	ID           string
	Name         string
	Description  string
	TemporalName string
	NewRequest   func() proto.Message
	NewResult    func() proto.Message
	Example      proto.Message
	WorkflowID   func(proto.Message) (string, error)
}

// Definitions returns the shared start contracts. The catalog is built once and every
// caller sees the same entries, so an entry and its Example must be treated as read-only.
func Definitions() []Definition { return definitions() }

// FindDefinition resolves a product ID. It reads a prepared index because the catalog is
// on the start, monitoring and Query paths, where rebuilding it per lookup would allocate
// every request's worth of example messages to answer one question.
func FindDefinition(id string) (Definition, bool) {
	definition, found := definitionsByID()[id]
	return definition, found
}

var definitions = sync.OnceValue(buildDefinitions)

var definitionsByID = sync.OnceValue(func() map[string]Definition {
	index := make(map[string]Definition, len(definitions()))
	for _, definition := range definitions() {
		index[definition.ID] = definition
	}
	return index
})

func buildDefinitions() []Definition {
	return []Definition{
		{
			ID: "greeting", Name: "Greeting", Description: "Run one Activity and return a greeting.",
			TemporalName: GreetingWorkflowName,
			NewRequest:   func() proto.Message { return &orchestrationv1.GreetingRequest{} },
			NewResult:    func() proto.Message { return &orchestrationv1.GreetingResult{} },
			Example:      &orchestrationv1.GreetingRequest{Name: "Temporal"},
		},
		{
			ID: "simple-diamond", Name: "Simple Diamond", Description: "Prepare, fan out to two parallel branches, then finalize.",
			TemporalName: SimpleDiamondWorkflowName,
			NewRequest:   func() proto.Message { return &orchestrationv1.SimpleDiamondRequest{} },
			NewResult:    func() proto.Message { return &orchestrationv1.SimpleDiamondResult{} },
			Example:      &orchestrationv1.SimpleDiamondRequest{PrepareDuration: durationpb.New(time.Second), BranchADuration: durationpb.New(2 * time.Second), BranchBDuration: durationpb.New(3 * time.Second), FinalizeDuration: durationpb.New(time.Second)},
		},
		{
			ID: "dynamic-fan-out", Name: "Dynamic Fan-Out", Description: "Plan a runtime branch count, wait on each branch, then finalize.",
			TemporalName: DynamicFanOutWorkflowName,
			NewRequest:   func() proto.Message { return &orchestrationv1.DynamicFanOutRequest{} },
			NewResult:    func() proto.Message { return &orchestrationv1.DynamicFanOutResult{} },
			Example:      &orchestrationv1.DynamicFanOutRequest{RequestedCount: 10, BranchDuration: durationpb.New(time.Second), FinalizeDuration: durationpb.New(time.Second)},
		},
		{
			ID: "reusable-artifacts", Name: "Reusable Activity Artifacts", Description: "Generate five expensive artifacts and reuse published results across retries and equivalent runs.",
			TemporalName: ReusableArtifactWorkflowName,
			NewRequest:   func() proto.Message { return &orchestrationv1.ReusableArtifactRequest{} },
			NewResult:    func() proto.Message { return &orchestrationv1.ReusableArtifactResult{} },
			Example: &orchestrationv1.ReusableArtifactRequest{
				ExperimentId:          "artifact-demo-001",
				ActivityVersion:       "v1",
				HeavyWorkDuration:     durationpb.New(20 * time.Second),
				FailureCase:           orchestrationv1.ReusableArtifactFailureCase_REUSABLE_ARTIFACT_FAILURE_CASE_NONE,
				FailureTargetActivity: "artifact-002",
			},
			WorkflowID: reusableArtifactDefinitionWorkflowID,
		},
		{
			ID: "durable-report", Name: "Durable Report", Description: "Aggregate reusable RustFS artifacts and persist one idempotent business report in SQLite.",
			TemporalName: DurableReportWorkflowName,
			NewRequest:   func() proto.Message { return &orchestrationv1.DurableReportRequest{} },
			NewResult:    func() proto.Message { return &orchestrationv1.DurableReportResult{} },
			Example: &orchestrationv1.DurableReportRequest{
				ExperimentId:      "report-experiment-001",
				ReportId:          "report-1001",
				ActivityVersion:   "v1",
				HeavyWorkDuration: durationpb.New(20 * time.Second),
				FailureCase:       orchestrationv1.DurableReportFailureCase_DURABLE_REPORT_FAILURE_CASE_NONE,
			},
			WorkflowID: durableReportDefinitionWorkflowID,
		},
		{
			ID: "fan-out-policy", Name: "Fan-Out Policy", Description: "Run fault-injection Activities using the selected aggregation policy.",
			TemporalName: FanOutPolicyWorkflowName,
			NewRequest:   func() proto.Message { return &orchestrationv1.FanOutPolicyRequest{} },
			NewResult:    func() proto.Message { return &orchestrationv1.FanOutPolicyResult{} },
			Example:      fanOutPolicyExample(),
		},
		{
			ID: "conditional-branch", Name: "Conditional Branch", Description: "Check inventory at runtime, then fulfill or backorder — exactly one path Activity.",
			TemporalName: ConditionalBranchWorkflowName,
			NewRequest:   func() proto.Message { return &orchestrationv1.ConditionalBranchRequest{} },
			NewResult:    func() proto.Message { return &orchestrationv1.ConditionalBranchResult{} },
			Example: &orchestrationv1.ConditionalBranchRequest{
				OrderId:        "order-1001",
				Sku:            "sku-widget",
				Quantity:       2,
				AvailableStock: 5,
			},
		},
	}
}

func ReusableArtifactWorkflowID(input *orchestrationv1.ReusableArtifactRequest) (string, error) {
	if _, err := ValidateReusableArtifactRequest(input); err != nil {
		return "", err
	}
	return "reusable-artifacts/" + input.ExperimentId, nil
}

func DurableReportWorkflowID(input *orchestrationv1.DurableReportRequest) (string, error) {
	if _, err := ValidateDurableReportRequest(input); err != nil {
		return "", err
	}
	return "durable-report/" + input.ExperimentId, nil
}

func ValidateReusableArtifactRequest(input *orchestrationv1.ReusableArtifactRequest) (time.Duration, error) {
	if input == nil {
		return 0, errors.New("input is required")
	}
	heavyWorkDuration, err := validateArtifactGenerationInput(input.ExperimentId, input.ActivityVersion, input.HeavyWorkDuration)
	if err != nil {
		return 0, err
	}
	switch input.FailureCase {
	case orchestrationv1.ReusableArtifactFailureCase_REUSABLE_ARTIFACT_FAILURE_CASE_NONE:
	case orchestrationv1.ReusableArtifactFailureCase_REUSABLE_ARTIFACT_FAILURE_CASE_BEFORE_PUBLICATION,
		orchestrationv1.ReusableArtifactFailureCase_REUSABLE_ARTIFACT_FAILURE_CASE_AFTER_PUBLICATION:
		if !validReusableArtifactID(input.FailureTargetActivity) {
			return 0, errors.New("failure target must be one of artifact-000 through artifact-004")
		}
	default:
		return 0, errors.New("failure case must be none, before publication, or after publication")
	}
	return heavyWorkDuration, nil
}

func ValidateDurableReportRequest(input *orchestrationv1.DurableReportRequest) (time.Duration, error) {
	if input == nil {
		return 0, errors.New("input is required")
	}
	heavyWorkDuration, err := validateArtifactGenerationInput(input.ExperimentId, input.ActivityVersion, input.HeavyWorkDuration)
	if err != nil {
		return 0, err
	}
	if !validIdentifier(input.ReportId) {
		return 0, errors.New("report ID must be 1-128 characters using letters, numbers, dot, underscore, or hyphen")
	}
	switch input.FailureCase {
	case orchestrationv1.DurableReportFailureCase_DURABLE_REPORT_FAILURE_CASE_NONE,
		orchestrationv1.DurableReportFailureCase_DURABLE_REPORT_FAILURE_CASE_AGGREGATION_RETRYABLE,
		orchestrationv1.DurableReportFailureCase_DURABLE_REPORT_FAILURE_CASE_PERSIST_BEFORE_COMMIT,
		orchestrationv1.DurableReportFailureCase_DURABLE_REPORT_FAILURE_CASE_PERSIST_AFTER_COMMIT:
	default:
		return 0, errors.New("failure case must be none, aggregation retryable, persist before commit, or persist after commit")
	}
	return heavyWorkDuration, nil
}

func ArtifactActivityID(index int) string {
	return fmt.Sprintf("artifact-%03d", index)
}

func ArtifactActivityIDs(count int) string {
	ids := make([]string, count)
	for index := range ids {
		ids[index] = ArtifactActivityID(index)
	}
	return strings.Join(ids, ",")
}

func reusableArtifactDefinitionWorkflowID(message proto.Message) (string, error) {
	input, ok := message.(*orchestrationv1.ReusableArtifactRequest)
	if !ok {
		return "", fmt.Errorf("expected ReusableArtifactRequest, got %T", message)
	}
	return ReusableArtifactWorkflowID(input)
}

func durableReportDefinitionWorkflowID(message proto.Message) (string, error) {
	input, ok := message.(*orchestrationv1.DurableReportRequest)
	if !ok {
		return "", fmt.Errorf("expected DurableReportRequest, got %T", message)
	}
	return DurableReportWorkflowID(input)
}

func fanOutPolicyExample() *orchestrationv1.FanOutPolicyRequest {
	return &orchestrationv1.FanOutPolicyRequest{
		Policy: orchestrationv1.AggregationPolicy_AGGREGATION_POLICY_ALL_SETTLED,
		// background_probabilities is omitted so the Workflow's documented default applies.
		Campaign: &orchestrationv1.FaultCampaignSpec{
			Type:          orchestrationv1.FaultCampaignType_FAULT_CAMPAIGN_TYPE_MIXED_V1,
			ActivityCount: 1000,
			Seed:          4815162342,
		},
	}
}

func validateArtifactGenerationInput(experimentID, activityVersion string, heavyWork *durationpb.Duration) (time.Duration, error) {
	if !validIdentifier(experimentID) {
		return 0, errors.New("experiment ID must be 1-128 characters using letters, numbers, dot, underscore, or hyphen")
	}
	if activityVersion == "" || activityVersion != strings.TrimSpace(activityVersion) {
		return 0, errors.New("activity version is required and must not have surrounding whitespace")
	}
	if heavyWork == nil || heavyWork.CheckValid() != nil {
		return 0, errors.New("heavy-work duration must be a valid protobuf duration")
	}
	heavyWorkDuration := heavyWork.AsDuration()
	if heavyWorkDuration <= 0 {
		return 0, errors.New("heavy-work duration must be positive")
	}
	if heavyWorkDuration > time.Duration(1<<63-1)-ArtifactActivityTimeoutMargin {
		return 0, errors.New("heavy-work duration is too large to configure a safe Activity timeout")
	}
	return heavyWorkDuration, nil
}

func validIdentifier(value string) bool {
	if len(value) == 0 || len(value) > 128 || value != strings.TrimSpace(value) {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '.' || character == '_' || character == '-' {
			continue
		}
		return false
	}
	return true
}

func validReusableArtifactID(value string) bool {
	for index := 0; index < ReusableArtifactCount; index++ {
		if value == ArtifactActivityID(index) {
			return true
		}
	}
	return false
}
