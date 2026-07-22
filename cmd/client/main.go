package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.temporal.io/sdk/client"

	"orchestration/internal/workflows"
)

const taskQueue = "dynamic-fan-out-1000"

func main() {
	c, err := client.Dial(client.Options{HostPort: temporalAddress()})
	if err != nil {
		log.Fatalf("connect to Temporal frontend: %v", err)
	}
	defer c.Close()

	workflowID := fmt.Sprintf("dynamic-fan-out-1000-%d", time.Now().UnixNano())
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: taskQueue,
	}, workflows.DynamicFanOutWorkflowName, workflows.DynamicFanOutInput{
		RequestedCount:   1000,
		BranchDuration:   1 * time.Second,
		FinalizeDuration: 1 * time.Second,
	})
	if err != nil {
		log.Fatalf("start workflow: %v", err)
	}

	var result workflows.DynamicFanOutResult
	if err := run.Get(context.Background(), &result); err != nil {
		log.Fatalf("wait for workflow %s: %v", workflowID, err)
	}

	fmt.Printf("Workflow ID: %s\nRun ID: %s\n", run.GetID(), run.GetRunID())
	fmt.Printf("Planned: %d, completed: %d, peak concurrency: %d\n", result.PlannedCount, result.CompletedCount, result.PeakConcurrentBranches)
	fmt.Printf("Workflow elapsed: %s\nBranch window: %s to %s\n",
		result.Elapsed,
		result.FirstBranchStartedAt.Format(time.RFC3339Nano),
		result.LastBranchFinishedAt.Format(time.RFC3339Nano),
	)
	fmt.Printf("Finalize: start=%s finish=%s elapsed=%s\n",
		result.Finalize.StartedAt.Format(time.RFC3339Nano),
		result.Finalize.FinishedAt.Format(time.RFC3339Nano),
		result.Finalize.Elapsed,
	)
	fmt.Printf("Temporal UI: http://localhost:8234/namespaces/default/workflows/%s/%s/history\n", run.GetID(), run.GetRunID())
}

func temporalAddress() string {
	if address := os.Getenv("TEMPORAL_ADDRESS"); address != "" {
		return address
	}
	return "localhost:7234"
}
