package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	orchestrationv1 "orchestration/gen/orchestration/v1"
	"orchestration/internal/workflows"
)

func (s *server) executeWorkflow(ctx context.Context, key string, rawInput json.RawMessage) (*runResponse, error) {
	definition, ok := workflows.FindDefinition(key)
	if !ok {
		return nil, &inputError{message: fmt.Sprintf("Unknown workflow id %q.", key)}
	}

	input := definition.NewRequest()
	if err := protojson.Unmarshal(rawInput, input); err != nil {
		return nil, &inputError{message: fmt.Sprintf("Invalid workflow input: %s", sanitizeDecodeError(err))}
	}

	startedAt := time.Now().UTC()
	run, err := s.temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        uniqueWorkflowID(key),
		TaskQueue: s.taskQueue,
	}, definition.TemporalName, input)
	if err != nil {
		return nil, &startError{message: "Failed to start the workflow.", cause: err}
	}

	result := definition.NewResult()
	workflowErr := run.Get(ctx, result)
	if workflowErr != nil {
		extractFailureResult(workflowErr, result)
	}
	return s.buildResponse(key, run, startedAt, result, workflowErr)
}

func (s *server) buildResponse(key string, run client.WorkflowRun, started time.Time, result proto.Message, workflowErr error) (*runResponse, error) {
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

	if workflowErr == nil {
		encoded, err := protojson.Marshal(result)
		if err != nil {
			return nil, fmt.Errorf("encode workflow result: %w", err)
		}
		response.Status = "completed"
		response.Result = encoded
		return response, nil
	}

	failure := toFailure(workflowErr)
	encodedFailure, err := protojson.Marshal(failure)
	if err != nil {
		return nil, fmt.Errorf("encode workflow failure: %w", err)
	}
	response.Status = "failed"
	response.Failure = encodedFailure
	if encodedResult, err := protojson.Marshal(result); err == nil && string(encodedResult) != "{}" {
		response.Result = encodedResult
	}
	return response, nil
}

func extractFailureResult(err error, result proto.Message) {
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || !appErr.HasDetails() {
		return
	}
	var failure orchestrationv1.WorkflowFailure
	if appErr.Details(&failure, result) != nil {
		_ = appErr.Details(result)
	}
}

func toFailure(err error) *orchestrationv1.WorkflowFailure {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		var failure orchestrationv1.WorkflowFailure
		if appErr.HasDetails() && appErr.Details(&failure) == nil && failure.Code != "" {
			return &failure
		}
		return &orchestrationv1.WorkflowFailure{
			Code:      "DEPENDENCY_FAILURE",
			Message:   "A workflow dependency failed.",
			Category:  orchestrationv1.FailureCategory_FAILURE_CATEGORY_DEPENDENCY,
			Retryable: !appErr.NonRetryable(),
			Metadata:  map[string]string{"temporalType": appErr.Type()},
		}
	}
	var timeoutErr *temporal.TimeoutError
	if errors.As(err, &timeoutErr) {
		return &orchestrationv1.WorkflowFailure{Code: "TIMEOUT", Message: "The workflow timed out.", Category: orchestrationv1.FailureCategory_FAILURE_CATEGORY_TIMEOUT, Retryable: true}
	}
	var canceledErr *temporal.CanceledError
	if errors.As(err, &canceledErr) {
		return &orchestrationv1.WorkflowFailure{Code: "CANCELED", Message: "The workflow was canceled.", Category: orchestrationv1.FailureCategory_FAILURE_CATEGORY_CANCELED}
	}
	return &orchestrationv1.WorkflowFailure{Code: "INTERNAL", Message: "The workflow could not be completed.", Category: orchestrationv1.FailureCategory_FAILURE_CATEGORY_INTERNAL}
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

type inputError struct{ message string }

func (e *inputError) Error() string { return e.message }

type startError struct {
	message string
	cause   error
}

func (e *startError) Error() string { return e.message }
func (e *startError) Unwrap() error { return e.cause }
