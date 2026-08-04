package workflows

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	orchestrationv1 "orchestration/gen/orchestration/v1"
)

type Definition struct {
	ID           string
	Name         string
	Description  string
	TemporalName string
	Workflow     any
	NewRequest   func() proto.Message
	NewResult    func() proto.Message
	Example      proto.Message
	WorkflowID   func(proto.Message) (string, error)
}

func Definitions() []Definition {
	return []Definition{
		{
			ID: "greeting", Name: "Greeting", Description: "Run one Activity and return a greeting.",
			TemporalName: GreetingWorkflowName, Workflow: GreetingWorkflow,
			NewRequest: func() proto.Message { return &orchestrationv1.GreetingRequest{} },
			NewResult:  func() proto.Message { return &orchestrationv1.GreetingResult{} },
			Example:    &orchestrationv1.GreetingRequest{Name: "Temporal"},
		},
		{
			ID: "simple-diamond", Name: "Simple Diamond", Description: "Prepare, fan out to two parallel branches, then finalize.",
			TemporalName: SimpleDiamondWorkflowName, Workflow: SimpleDiamondWorkflow,
			NewRequest: func() proto.Message { return &orchestrationv1.SimpleDiamondRequest{} },
			NewResult:  func() proto.Message { return &orchestrationv1.SimpleDiamondResult{} },
			Example:    &orchestrationv1.SimpleDiamondRequest{PrepareDuration: durationpb.New(time.Second), BranchADuration: durationpb.New(2 * time.Second), BranchBDuration: durationpb.New(3 * time.Second), FinalizeDuration: durationpb.New(time.Second)},
		},
		{
			ID: "dynamic-fan-out", Name: "Dynamic Fan-Out", Description: "Plan a runtime branch count, wait on each branch, then finalize.",
			TemporalName: DynamicFanOutWorkflowName, Workflow: DynamicFanOutWorkflow,
			NewRequest: func() proto.Message { return &orchestrationv1.DynamicFanOutRequest{} },
			NewResult:  func() proto.Message { return &orchestrationv1.DynamicFanOutResult{} },
			Example:    &orchestrationv1.DynamicFanOutRequest{RequestedCount: 10, BranchDuration: durationpb.New(time.Second), FinalizeDuration: durationpb.New(time.Second)},
		},
		{
			ID: "reusable-artifacts", Name: "Reusable Activity Artifacts", Description: "Generate five expensive artifacts and reuse published results across retries and equivalent runs.",
			TemporalName: ReusableArtifactWorkflowName, Workflow: ReusableArtifactWorkflow,
			NewRequest: func() proto.Message { return &orchestrationv1.ReusableArtifactRequest{} },
			NewResult:  func() proto.Message { return &orchestrationv1.ReusableArtifactResult{} },
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
			TemporalName: DurableReportWorkflowName, Workflow: DurableReportWorkflow,
			NewRequest: func() proto.Message { return &orchestrationv1.DurableReportRequest{} },
			NewResult:  func() proto.Message { return &orchestrationv1.DurableReportResult{} },
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
			TemporalName: FanOutPolicyWorkflowName, Workflow: FanOutPolicyWorkflow,
			NewRequest: func() proto.Message { return &orchestrationv1.FanOutPolicyRequest{} },
			NewResult:  func() proto.Message { return &orchestrationv1.FanOutPolicyResult{} },
			Example:    fanOutPolicyExample(),
		},
		{
			ID: "conditional-branch", Name: "Conditional Branch", Description: "Check inventory at runtime, then fulfill or backorder — exactly one path Activity.",
			TemporalName: ConditionalBranchWorkflowName, Workflow: ConditionalBranchWorkflow,
			NewRequest: func() proto.Message { return &orchestrationv1.ConditionalBranchRequest{} },
			NewResult:  func() proto.Message { return &orchestrationv1.ConditionalBranchResult{} },
			Example: &orchestrationv1.ConditionalBranchRequest{
				OrderId:        "order-1001",
				Sku:            "sku-widget",
				Quantity:       2,
				AvailableStock: 5,
			},
		},
	}
}

func FindDefinition(id string) (Definition, bool) {
	for _, definition := range Definitions() {
		if definition.ID == id {
			return definition, true
		}
	}
	return Definition{}, false
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
		Campaign: &orchestrationv1.FaultCampaignSpec{
			Type:          orchestrationv1.FaultCampaignType_FAULT_CAMPAIGN_TYPE_MIXED_V1,
			ActivityCount: 1000,
			Seed:          4815162342,
			BackgroundProbabilities: &orchestrationv1.OutcomeProbabilities{
				Success:             82,
				RetryableFailure:    8,
				NonRetryableFailure: 3,
				Panic:               2,
				StartToCloseTimeout: 3,
				HeartbeatTimeout:    2,
			},
		},
	}
}
