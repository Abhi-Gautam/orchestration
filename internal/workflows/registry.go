package workflows

import (
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
			ID: "fan-out-policy", Name: "Fan-Out Policy", Description: "Run fault-injection Activities using the selected aggregation policy.",
			TemporalName: FanOutPolicyWorkflowName, Workflow: FanOutPolicyWorkflow,
			NewRequest: func() proto.Message { return &orchestrationv1.FanOutPolicyRequest{} },
			NewResult:  func() proto.Message { return &orchestrationv1.FanOutPolicyResult{} },
			Example:    fanOutPolicyExample(),
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

func fanOutPolicyExample() *orchestrationv1.FanOutPolicyRequest {
	branches := make([]*orchestrationv1.FaultBranchSpec, 3)
	for i := range branches {
		mode := orchestrationv1.FaultMode_FAULT_MODE_SUCCESS
		if i == 1 {
			mode = orchestrationv1.FaultMode_FAULT_MODE_NON_RETRYABLE_FAILURE
		}
		branches[i] = &orchestrationv1.FaultBranchSpec{Name: []string{"branch-00", "branch-01", "branch-02"}[i], Mode: mode, WorkDuration: durationpb.New(time.Second), HeartbeatInterval: durationpb.New(100 * time.Millisecond)}
	}
	return &orchestrationv1.FanOutPolicyRequest{Policy: orchestrationv1.AggregationPolicy_AGGREGATION_POLICY_ALL_SETTLED, Branches: branches, FinalizeDuration: durationpb.New(500 * time.Millisecond)}
}
