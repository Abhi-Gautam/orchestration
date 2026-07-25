package main

import (
	"log"
	"os"
	"strings"

	"github.com/joho/godotenv"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"orchestration/internal/activities"
	"orchestration/internal/workflows"
)

func main() {
	_ = godotenv.Load()

	temporalAddress := requiredEnv("TEMPORAL_ADDRESS")
	namespace := requiredEnv("TEMPORAL_NAMESPACE")
	taskQueue := requiredEnv("TASK_QUEUE")

	c, err := client.Dial(client.Options{HostPort: temporalAddress, Namespace: namespace})
	if err != nil {
		log.Fatalf("connect to Temporal frontend: %v", err)
	}
	defer c.Close()

	w := worker.New(c, taskQueue, worker.Options{})
	for _, definition := range workflows.Definitions() {
		w.RegisterWorkflowWithOptions(definition.Workflow, workflow.RegisterOptions{Name: definition.TemporalName})
	}
	w.RegisterActivity(activities.FormatGreeting)
	w.RegisterActivity(activities.WaitActivity)
	w.RegisterActivity(activities.PlanFanOutActivity)
	w.RegisterActivity(activities.FaultInjectionActivity)

	log.Printf("worker listening on task queue %q (temporal %s)", taskQueue, temporalAddress)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("run worker: %v", err)
	}
}

func requiredEnv(key string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		log.Fatalf("required environment variable %s is not set", key)
	}
	return value
}
