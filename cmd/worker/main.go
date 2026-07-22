package main

import (
	"log"
	"os"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"orchestration/internal/activities"
	"orchestration/internal/workflows"
)

const taskQueue = "learning-dags"

func main() {
	c, err := client.Dial(client.Options{HostPort: temporalAddress()})
	if err != nil {
		log.Fatalf("connect to Temporal frontend: %v", err)
	}
	defer c.Close()

	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(workflows.GreetingWorkflow, workflow.RegisterOptions{Name: workflows.GreetingWorkflowName})
	w.RegisterActivity(activities.FormatGreeting)

	log.Printf("worker listening on task queue %q", taskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("run worker: %v", err)
	}
}

func temporalAddress() string {
	if address := os.Getenv("TEMPORAL_ADDRESS"); address != "" {
		return address
	}
	return "localhost:7234"
}
