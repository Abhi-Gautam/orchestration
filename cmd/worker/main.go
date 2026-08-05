package main

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/joho/godotenv"

	temporalactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"orchestration/internal/activities"
	"orchestration/internal/workflowcatalog"
	"orchestration/internal/workflows"
)

func main() {
	_ = godotenv.Load()

	temporalAddress := requiredEnv("TEMPORAL_ADDRESS")
	namespace := requiredEnv("TEMPORAL_NAMESPACE")
	taskQueue := requiredEnv("TASK_QUEUE")
	artifactActivities, err := activities.NewArtifactActivities(context.Background(), activities.ArtifactStoreConfig{
		Endpoint:  requiredEnv("RUSTFS_ENDPOINT_URL"),
		Region:    requiredEnv("RUSTFS_REGION"),
		AccessKey: requiredEnv("RUSTFS_ACCESS_KEY_ID"),
		SecretKey: requiredEnv("RUSTFS_SECRET_ACCESS_KEY"),
		Bucket:    requiredEnv("RUSTFS_ARTIFACT_BUCKET"),
	})
	if err != nil {
		log.Fatalf("initialize RustFS artifact store: %v", err)
	}
	reportActivities, err := activities.NewReportActivities(context.Background(), activities.ReportStoreConfig{
		Path: requiredEnv("APPLICATION_DB_PATH"),
	})
	if err != nil {
		log.Fatalf("initialize SQLite report store: %v", err)
	}

	c, err := client.Dial(client.Options{HostPort: temporalAddress, Namespace: namespace})
	if err != nil {
		log.Fatalf("connect to Temporal frontend: %v", err)
	}
	defer c.Close()

	w := worker.New(c, taskQueue, worker.Options{})
	workflowExecutions := workflows.Executions()
	for _, definition := range workflowcatalog.Definitions() {
		execution, ok := workflowExecutions[definition.TemporalName]
		if !ok {
			log.Fatalf("workflow catalog entry %q has no executable implementation", definition.TemporalName)
		}
		w.RegisterWorkflowWithOptions(execution, workflow.RegisterOptions{Name: definition.TemporalName})
		delete(workflowExecutions, definition.TemporalName)
	}
	if len(workflowExecutions) != 0 {
		log.Fatalf("%d executable Workflow implementations are missing from the catalog", len(workflowExecutions))
	}
	w.RegisterActivity(activities.FormatGreeting)
	w.RegisterActivity(activities.WaitActivity)
	w.RegisterActivity(activities.PlanFanOutActivity)
	w.RegisterActivity(activities.FaultInjectionActivity)
	w.RegisterActivity(activities.CheckInventory)
	w.RegisterActivity(activities.FulfillOrder)
	w.RegisterActivity(activities.BackorderOrder)
	w.RegisterActivityWithOptions(
		artifactActivities.GenerateArtifactActivity,
		temporalactivity.RegisterOptions{Name: activities.GenerateArtifactActivityName},
	)
	w.RegisterActivityWithOptions(
		artifactActivities.AggregateArtifactsActivity,
		temporalactivity.RegisterOptions{Name: activities.AggregateArtifactsActivityName},
	)
	w.RegisterActivityWithOptions(
		reportActivities.PersistReportActivity,
		temporalactivity.RegisterOptions{Name: activities.PersistReportActivityName},
	)

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
