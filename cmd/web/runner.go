package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	"orchestration/internal/workflows"
)

func (s *server) executeWorkflow(ctx context.Context, key string, raw json.RawMessage) (*runResponse, error) {
	startedAt := time.Now().UTC()
	switch key {
	case "greeting":
		return s.runGreeting(ctx, raw, startedAt)
	case "simple-diamond":
		return s.runSimpleDiamond(ctx, raw, startedAt)
	case "dynamic-fan-out":
		return s.runDynamicFanOut(ctx, raw, startedAt)
	case "fan-out-policy":
		return s.runFanOutPolicy(ctx, raw, startedAt)
	default:
		return nil, &inputError{message: fmt.Sprintf("Unknown workflow id %q.", key)}
	}
}

func (s *server) runGreeting(ctx context.Context, raw json.RawMessage, started time.Time) (*runResponse, error) {
	var input greetingWebInput
	if err := decodeInput(raw, &input); err != nil {
		return nil, err
	}
	if strings.TrimSpace(input.Name) == "" {
		return nil, &inputError{message: "Input field \"name\" is required."}
	}
	run, err := s.start(ctx, "greeting", workflows.GreetingWorkflowName, input.Name)
	if err != nil {
		return nil, err
	}
	var result string
	workflowErr := run.Get(ctx, &result)
	return s.buildResponse("greeting", run, started, result, workflowErr), nil
}
func (s *server) runSimpleDiamond(ctx context.Context, raw json.RawMessage, started time.Time) (*runResponse, error) {
	var input simpleDiamondWebInput
	if err := decodeInput(raw, &input); err != nil {
		return nil, err
	}
	workflowInput := workflows.SimpleDiamondInput{PrepareDuration: input.PrepareDuration.Duration(), BranchADuration: input.BranchADuration.Duration(), BranchBDuration: input.BranchBDuration.Duration(), FinalizeDuration: input.FinalizeDuration.Duration()}
	if workflowInput.PrepareDuration < 0 || workflowInput.BranchADuration < 0 || workflowInput.BranchBDuration < 0 || workflowInput.FinalizeDuration < 0 {
		return nil, &inputError{message: "Durations must not be negative."}
	}
	run, err := s.start(ctx, "simple-diamond", workflows.SimpleDiamondWorkflowName, workflowInput)
	if err != nil {
		return nil, err
	}
	var result workflows.SimpleDiamondResult
	workflowErr := run.Get(ctx, &result)
	return s.buildResponse("simple-diamond", run, started, result, workflowErr), nil
}
func (s *server) runDynamicFanOut(ctx context.Context, raw json.RawMessage, started time.Time) (*runResponse, error) {
	var input dynamicFanOutWebInput
	if err := decodeInput(raw, &input); err != nil {
		return nil, err
	}
	if input.RequestedCount <= 0 || input.RequestedCount > 1000 {
		return nil, &inputError{message: "Input field \"requestedCount\" must be between 1 and 1000."}
	}
	if input.BranchDuration.Duration() < 0 || input.FinalizeDuration.Duration() < 0 {
		return nil, &inputError{message: "Durations must not be negative."}
	}
	workflowInput := workflows.DynamicFanOutInput{RequestedCount: input.RequestedCount, BranchDuration: input.BranchDuration.Duration(), FinalizeDuration: input.FinalizeDuration.Duration()}
	run, err := s.start(ctx, "dynamic-fan-out", workflows.DynamicFanOutWorkflowName, workflowInput)
	if err != nil {
		return nil, err
	}
	var result workflows.DynamicFanOutResult
	workflowErr := run.Get(ctx, &result)
	return s.buildResponse("dynamic-fan-out", run, started, result, workflowErr), nil
}
func (s *server) start(ctx context.Context, prefix, workflowName string, input any) (client.WorkflowRun, error) {
	run, err := s.temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: uniqueWorkflowID(prefix), TaskQueue: s.taskQueue}, workflowName, input)
	if err != nil {
		return nil, &startError{message: "Failed to start the workflow.", cause: err}
	}
	return run, nil
}
func (s *server) buildResponse(key string, run client.WorkflowRun, started time.Time, result any, workflowErr error) *runResponse {
	finished := time.Now().UTC()
	response := &runResponse{Workflow: key, WorkflowID: run.GetID(), RunID: run.GetRunID(), StartedAt: started, FinishedAt: finished, Elapsed: formatElapsed(finished.Sub(started)), TemporalUIURL: fmt.Sprintf("%s/namespaces/%s/workflows/%s/%s/history", s.temporalUI, s.namespace, run.GetID(), run.GetRunID())}
	if workflowErr != nil {
		response.Status = "failed"
		response.Failure = toFailure(workflowErr)
	} else {
		response.Status = "completed"
		response.Result = result
	}
	return response
}
func uniqueWorkflowID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}
func formatElapsed(d time.Duration) string {
	if d < time.Millisecond {
		return d.String()
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Round(time.Millisecond)/time.Millisecond)
	}
	if d < time.Minute {
		return d.Round(10 * time.Millisecond).String()
	}
	return d.Round(time.Second).String()
}
func toFailure(err error) *failure {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		kind := appErr.Type()
		if kind == "" {
			kind = "ApplicationFailure"
		}
		message := appErr.Message()
		if message == "" {
			message = "The workflow failed with an application error."
		}
		return &failure{Type: kind, Message: message}
	}
	var timeoutErr *temporal.TimeoutError
	if errors.As(err, &timeoutErr) {
		return &failure{Type: "TimeoutFailure", Message: timeoutErr.Error()}
	}
	var canceledErr *temporal.CanceledError
	if errors.As(err, &canceledErr) {
		return &failure{Type: "CanceledFailure", Message: "The workflow was canceled."}
	}
	message := err.Error()
	if cause := errors.Unwrap(err); cause != nil {
		message = cause.Error()
	}
	if index := strings.Index(message, "\n"); index >= 0 {
		message = message[:index]
	}
	if len(message) > 300 {
		message = message[:300] + "…"
	}
	return &failure{Type: "WorkflowFailure", Message: message}
}

type inputError struct{ message string }

func (e *inputError) Error() string { return e.message }

type startError struct {
	message string
	cause   error
}

func (e *startError) Error() string { return e.message }
func (e *startError) Unwrap() error { return e.cause }
