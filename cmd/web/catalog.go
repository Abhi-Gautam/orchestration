package main

import (
	"fmt"
	"time"

	"orchestration/internal/workflows"
)

func workflowCatalog() []catalogWorkflow {
	return []catalogWorkflow{
		{
			ID:           "greeting",
			Name:         "Greeting",
			Description:  "Run one Activity and return a greeting.",
			ExampleInput: map[string]any{"Name": "Temporal"},
			workflowName: workflows.GreetingWorkflowName,
		},
		{
			ID:          "simple-diamond",
			Name:        "Simple Diamond",
			Description: "Prepare, fan out to two parallel branches, then finalize.",
			ExampleInput: map[string]any{
				"PrepareDuration":  time.Second,
				"BranchADuration":  2 * time.Second,
				"BranchBDuration":  3 * time.Second,
				"FinalizeDuration": time.Second,
			},
			workflowName: workflows.SimpleDiamondWorkflowName,
		},
		{
			ID:          "dynamic-fan-out",
			Name:        "Dynamic Fan-Out",
			Description: "Plan a runtime branch count, wait on each branch, then finalize.",
			ExampleInput: map[string]any{
				"RequestedCount":   10,
				"BranchDuration":   time.Second,
				"FinalizeDuration": time.Second,
			},
			workflowName: workflows.DynamicFanOutWorkflowName,
		},
		{
			ID:           "fan-out-policy",
			Name:         "Fan-Out Policy",
			Description:  "Run fault-injection Activities using the selected aggregation policy.",
			ExampleInput: fanOutPolicyExample(),
			workflowName: workflows.FanOutPolicyWorkflowName,
		},
	}
}

func fanOutPolicyExample() map[string]any {
	branches := make([]map[string]any, 3)
	for i := range branches {
		mode := "success"
		if i == 1 {
			mode = "non-retryable-failure"
		}
		id := fmt.Sprintf("branch-%02d", i)
		branches[i] = map[string]any{
			"ActivityID": id,
			"Input": map[string]any{
				"Name":              id,
				"Mode":              mode,
				"WorkDuration":      time.Second,
				"HeartbeatInterval": 100 * time.Millisecond,
			},
			"StartToCloseTimeout":    5 * time.Second,
			"ScheduleToCloseTimeout": 10 * time.Second,
			"HeartbeatTimeout":       time.Second,
			"RetryPolicy": map[string]any{
				"InitialInterval":    100 * time.Millisecond,
				"BackoffCoefficient": 2,
				"MaximumInterval":    time.Second,
				"MaximumAttempts":    1,
			},
		}
	}
	return map[string]any{"Policy": "all-settled", "Branches": branches, "FinalizeDuration": 500 * time.Millisecond}
}
