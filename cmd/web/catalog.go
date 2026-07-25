package main

func workflowCatalog() []catalogWorkflow {
	return []catalogWorkflow{
		{ID: "greeting", Name: "Greeting", Description: "Run one Activity and return a greeting.", ExampleInput: map[string]any{"name": "Temporal"}},
		{ID: "simple-diamond", Name: "Simple Diamond", Description: "Prepare, fan out to two parallel branches, then finalize.", ExampleInput: map[string]any{"prepareDuration": "1s", "branchADuration": "2s", "branchBDuration": "3s", "finalizeDuration": "1s"}},
		{ID: "dynamic-fan-out", Name: "Dynamic Fan-Out", Description: "Plan a runtime branch count, wait on each branch, then finalize.", ExampleInput: map[string]any{"requestedCount": 10, "branchDuration": "1s", "finalizeDuration": "1s"}},
		{ID: "fan-out-policy", Name: "Fan-Out Policy", Description: "Fan out fault-injection Activities and aggregate with fail-fast, all-settled, or all-settled-then-fail.", ExampleInput: map[string]any{"policy": "all-settled", "branchCount": 10, "failureBranch": 3, "faultMode": "non-retryable-failure"}},
	}
}
