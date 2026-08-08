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
	TrainingJobWorkflowName       = "TrainingJobWorkflow"
	TrainingShardWorkflowName     = "TrainingShardWorkflow"
	ConditionalBranchWorkflowName = "ConditionalBranchWorkflow"
	OperationStatusQueryName      = "operation-status"

	ReusableArtifactCount         = 5
	MaxTrainingShards             = 8
	MaxTrainingSteps              = 10000
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
			ID: "training-job", Name: "Training Job", Description: "Checkpointing shards under a parent that owns them; cancel drains within a grace period, terminate does not.",
			TemporalName: TrainingJobWorkflowName,
			NewRequest:   func() proto.Message { return &orchestrationv1.TrainingJobRequest{} },
			NewResult:    func() proto.Message { return &orchestrationv1.TrainingJobResult{} },
			Example: &orchestrationv1.TrainingJobRequest{
				ExperimentId:       "training-001",
				ShardCount:         3,
				TotalSteps:         60,
				StepsPerCheckpoint: 5,
				StepDuration:       durationpb.New(time.Second),
				ShutdownGrace:      durationpb.New(10 * time.Second),
				FailureCase:        orchestrationv1.CheckpointFailureCase_CHECKPOINT_FAILURE_CASE_NONE,
			},
			WorkflowID: trainingJobDefinitionWorkflowID,
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

// TrainingJobWorkflowID makes the experiment the job's identity, so repeating a request
// joins the run in flight and a request after cancellation resumes from its checkpoints.
func TrainingJobWorkflowID(input *orchestrationv1.TrainingJobRequest) (string, error) {
	if err := ValidateTrainingJobRequest(input); err != nil {
		return "", err
	}
	return "training-job/" + input.ExperimentId, nil
}

// TrainingShardWorkflowID keeps a shard's identity stable across resumes so its Workflow ID
// names the same logical shard every time the job runs.
func TrainingShardWorkflowID(experimentID string, shard int) string {
	return fmt.Sprintf("training-job/%s/shard-%02d", experimentID, shard)
}

func ShardID(shard int) string {
	return fmt.Sprintf("shard-%02d", shard)
}

func ValidateTrainingJobRequest(input *orchestrationv1.TrainingJobRequest) error {
	if input == nil {
		return errors.New("input is required")
	}
	if !validIdentifier(input.ExperimentId) {
		return errors.New("experiment ID must be 1-128 characters using letters, numbers, dot, underscore, or hyphen")
	}
	if input.ShardCount < 1 || input.ShardCount > MaxTrainingShards {
		return fmt.Errorf("shard_count must be between 1 and %d", MaxTrainingShards)
	}
	if input.TotalSteps < 1 || input.TotalSteps > MaxTrainingSteps {
		return fmt.Errorf("total_steps must be between 1 and %d", MaxTrainingSteps)
	}
	if input.StepsPerCheckpoint < 1 || input.StepsPerCheckpoint > input.TotalSteps {
		return errors.New("steps_per_checkpoint must be between 1 and total_steps")
	}
	if input.StepDuration == nil || input.StepDuration.AsDuration() <= 0 {
		return errors.New("step_duration must be positive")
	}
	if input.ShutdownGrace != nil && input.ShutdownGrace.AsDuration() < 0 {
		return errors.New("shutdown_grace must not be negative")
	}
	switch input.FailureCase {
	case orchestrationv1.CheckpointFailureCase_CHECKPOINT_FAILURE_CASE_NONE,
		orchestrationv1.CheckpointFailureCase_CHECKPOINT_FAILURE_CASE_TORN_WRITE:
	default:
		return errors.New("failure case must be none or torn write")
	}
	return nil
}

func trainingJobDefinitionWorkflowID(message proto.Message) (string, error) {
	input, ok := message.(*orchestrationv1.TrainingJobRequest)
	if !ok {
		return "", errors.New("unexpected request type")
	}
	return TrainingJobWorkflowID(input)
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

// DefaultMixedProbabilities is the mixed campaign's background outcome mix. The registered
// example and the Workflow's own default read it from here so the two cannot drift.
func DefaultMixedProbabilities() *orchestrationv1.OutcomeProbabilities {
	return &orchestrationv1.OutcomeProbabilities{
		Success:             82,
		RetryableFailure:    8,
		NonRetryableFailure: 3,
		Panic:               2,
		StartToCloseTimeout: 3,
		HeartbeatTimeout:    2,
	}
}

func fanOutPolicyExample() *orchestrationv1.FanOutPolicyRequest {
	return &orchestrationv1.FanOutPolicyRequest{
		Policy: orchestrationv1.AggregationPolicy_AGGREGATION_POLICY_ALL_SETTLED,
		Campaign: &orchestrationv1.FaultCampaignSpec{
			Type:                    orchestrationv1.FaultCampaignType_FAULT_CAMPAIGN_TYPE_MIXED_V1,
			ActivityCount:           1000,
			Seed:                    4815162342,
			BackgroundProbabilities: DefaultMixedProbabilities(),
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
