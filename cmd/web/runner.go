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
)

func (s *server) executeWorkflow(ctx context.Context, key string, rawInput json.RawMessage) (*runResponse, error) {
	entry, ok := findWorkflow(key)
	if !ok {
		return nil, &inputError{message: fmt.Sprintf("Unknown workflow id %q.", key)}
	}

	var input any
	if err := decodeInput(rawInput, &input); err != nil {
		return nil, err
	}

	startedAt := time.Now().UTC()
	run, err := s.temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        uniqueWorkflowID(key),
		TaskQueue: s.taskQueue,
	}, entry.workflowName, input)
	if err != nil {
		return nil, &startError{message: "Failed to start the workflow.", cause: err}
	}

	var result any
	workflowErr := run.Get(ctx, &result)
	return s.buildResponse(key, run, startedAt, result, workflowErr), nil
}

func findWorkflow(key string) (catalogWorkflow, bool) {
	for _, entry := range workflowCatalog() {
		if entry.ID == key {
			return entry, true
		}
	}
	return catalogWorkflow{}, false
}

func (s *server) buildResponse(key string, run client.WorkflowRun, started time.Time, result any, workflowErr error) *runResponse {
	finished := time.Now().UTC()
	response := &runResponse{
		Workflow:      key,
		WorkflowID:    run.GetID(),
		RunID:         run.GetRunID(),
		StartedAt:     started,
		FinishedAt:    finished,
		Elapsed:       formatElapsed(finished.Sub(started)),
		TemporalUIURL: fmt.Sprintf("%s/namespaces/%s/workflows/%s/%s/history", s.temporalUI, s.namespace, run.GetID(), run.GetRunID()),
	}
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
		kind, message := appErr.Type(), appErr.Message()
		if kind == "" {
			kind = "ApplicationFailure"
		}
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
