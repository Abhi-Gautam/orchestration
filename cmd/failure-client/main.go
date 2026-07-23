package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	"orchestration/internal/activities"
	"orchestration/internal/workflows"
)

const taskQueue = "fan-out-failure-policies"

func main() {
	policyValue := flag.String("policy", string(workflows.AllSettled), "fail-fast, all-settled, or all-settled-then-fail")
	flag.Parse()
	policy := workflows.AggregationPolicy(*policyValue)
	if policy != workflows.FailFast && policy != workflows.AllSettled && policy != workflows.AllSettledThenFail {
		log.Fatalf("unsupported policy %q", policy)
	}

	c, err := client.Dial(client.Options{HostPort: temporalAddress()})
	if err != nil {
		log.Fatalf("connect to Temporal frontend: %v", err)
	}
	defer c.Close()

	workflowID := fmt.Sprintf("fan-out-%s-%d", policy, time.Now().UnixNano())
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: taskQueue,
	}, workflows.FanOutPolicyWorkflowName, workflows.FanOutPolicyInput{
		Policy:           policy,
		Branches:         exampleBranches(),
		FinalizeDuration: 500 * time.Millisecond,
	})
	if err != nil {
		log.Fatalf("start workflow: %v", err)
	}

	var result workflows.FanOutResult
	err = run.Get(context.Background(), &result)
	fmt.Printf("Workflow ID: %s\nRun ID: %s\nPolicy: %s\n", run.GetID(), run.GetRunID(), policy)
	if err != nil {
		fmt.Printf("Workflow error: %v\n", err)
	} else {
		fmt.Printf("Planned=%d succeeded=%d failed=%d canceled=%d elapsed=%s\n",
			result.Planned,
			result.Succeeded,
			result.Failed,
			result.Canceled,
			result.Elapsed,
		)
		for _, outcome := range result.Outcomes {
			if outcome.Failure != nil {
				fmt.Printf("  %s: %s (%s)\n", outcome.ActivityID, outcome.Failure.Kind, outcome.Failure.Type)
			} else {
				fmt.Printf("  %s: success on attempt %d\n", outcome.ActivityID, outcome.Result.Attempt)
			}
		}
	}
	fmt.Printf("Temporal UI: http://localhost:8234/namespaces/default/workflows/%s/%s/history\n", run.GetID(), run.GetRunID())
}

func exampleBranches() []workflows.FaultBranch {
	branches := make([]workflows.FaultBranch, 10)
	for i := range branches {
		id := fmt.Sprintf("branch-%02d", i)
		mode := activities.FaultSuccess
		duration := 2 * time.Second
		if i == 3 {
			mode = activities.FaultNonRetryableFailure
			duration = 250 * time.Millisecond
		}
		branches[i] = workflows.FaultBranch{
			ActivityID: id,
			Input: activities.FaultActivityInput{
				Name:              id,
				Mode:              mode,
				WorkDuration:      duration,
				HeartbeatInterval: 100 * time.Millisecond,
			},
			StartToCloseTimeout:    5 * time.Second,
			ScheduleToCloseTimeout: 10 * time.Second,
			HeartbeatTimeout:       time.Second,
			RetryPolicy: temporal.RetryPolicy{
				InitialInterval:    100 * time.Millisecond,
				BackoffCoefficient: 2,
				MaximumInterval:    time.Second,
				MaximumAttempts:    1,
			},
		}
	}
	return branches
}

func temporalAddress() string {
	if address := os.Getenv("TEMPORAL_ADDRESS"); address != "" {
		return address
	}
	return "localhost:7234"
}
