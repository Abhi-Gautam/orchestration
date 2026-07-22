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

const taskQueue = "learning-dags"

func main() {
	c, err := client.Dial(client.Options{HostPort: temporalAddress()})
	if err != nil {
		log.Fatalf("connect to Temporal frontend: %v", err)
	}
	defer c.Close()

	workflowID := fmt.Sprintf("greeting-%d", time.Now().UnixNano())
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: taskQueue,
	}, workflows.GreetingWorkflowName, "Temporal learner")
	if err != nil {
		log.Fatalf("start workflow: %v", err)
	}

	var result string
	if err := run.Get(context.Background(), &result); err != nil {
		log.Fatalf("wait for workflow %s: %v", workflowID, err)
	}

	fmt.Printf("Workflow ID: %s\nRun ID: %s\nResult: %s\n", run.GetID(), run.GetRunID(), result)
}

func temporalAddress() string {
	if address := os.Getenv("TEMPORAL_ADDRESS"); address != "" {
		return address
	}
	return "localhost:7234"
}
