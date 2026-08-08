package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	orchestrationv1 "orchestration/gen/orchestration/v1"
	"orchestration/internal/workflowcatalog"
)

// startIdentity marks starts issued by the web process in Temporal's execution record.
const startIdentity = "orchestration-web"

func (s *server) startWorkflow(ctx context.Context, key string, rawInput json.RawMessage) (*runDescriptor, error) {
	definition, ok := workflowcatalog.FindDefinition(key)
	if !ok {
		return nil, &inputError{message: fmt.Sprintf("Unknown workflow id %q.", key)}
	}

	input := definition.NewRequest()
	if err := protojson.Unmarshal(rawInput, input); err != nil {
		return nil, &inputError{message: fmt.Sprintf("Invalid workflow input: %s", sanitizeDecodeError(err))}
	}

	request, err := s.startRequest(definition, input)
	if err != nil {
		return nil, err
	}

	startedAt := time.Now().UTC()
	response, err := s.temporal.WorkflowService().StartWorkflowExecution(ctx, request)
	if err != nil {
		return nil, &startError{message: "Failed to start the workflow.", cause: err}
	}

	return &runDescriptor{
		Workflow:      key,
		WorkflowName:  definition.Name,
		Status:        "running",
		WorkflowID:    request.WorkflowId,
		RunID:         response.GetRunId(),
		Attached:      !response.GetStarted(),
		StartedAt:     startedAt,
		TemporalUIURL: s.temporalRunURL(request.WorkflowId, response.GetRunId()),
	}, nil
}

// startRequest builds the start directly rather than through ExecuteWorkflow, because the
// SDK does not surface whether the server created a new execution or handed back one
// already running. For a business-keyed operation that answer is the point of the call, and
// only the server can give it without racing another caller.
func (s *server) startRequest(definition workflowcatalog.Definition, input proto.Message) (*workflowservice.StartWorkflowExecutionRequest, error) {
	workflowID := uniqueWorkflowID(definition.ID)
	conflictPolicy := enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL
	if definition.WorkflowID != nil {
		resolved, err := definition.WorkflowID(input)
		if err != nil {
			return nil, &inputError{message: fmt.Sprintf("Invalid workflow input: %s", err)}
		}
		// The Workflow ID is the operation's identity, so a second caller of work already
		// in flight joins that execution instead of starting a rival one beside it.
		workflowID = resolved
		conflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
	}

	payloads, err := converter.GetDefaultDataConverter().ToPayloads(input)
	if err != nil {
		return nil, fmt.Errorf("encode workflow input: %w", err)
	}
	requestID, err := newRequestID()
	if err != nil {
		return nil, err
	}

	return &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                s.namespace,
		WorkflowId:               workflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: definition.TemporalName},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: s.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:                    payloads,
		Identity:                 startIdentity,
		RequestId:                requestID,
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIdConflictPolicy: conflictPolicy,
	}, nil
}

// newRequestID gives every start its own request. Temporal treats a repeated request ID as
// a retry of the first call and replays its response, which would report a caller that
// joined an existing run as having started it.
func newRequestID() (string, error) {
	var buffer [16]byte
	if _, err := rand.Read(buffer[:]); err != nil {
		return "", fmt.Errorf("generate start request id: %w", err)
	}
	return hex.EncodeToString(buffer[:]), nil
}

func (s *server) awaitWorkflow(ctx context.Context, descriptor runDescriptor) (*runResponse, error) {
	definition, ok := workflowcatalog.FindDefinition(descriptor.Workflow)
	if !ok {
		return nil, &inputError{message: fmt.Sprintf("Unknown workflow id %q.", descriptor.Workflow)}
	}

	run := s.temporal.GetWorkflow(ctx, descriptor.WorkflowID, descriptor.RunID)
	result := definition.NewResult()
	workflowErr := run.Get(ctx, result)
	if workflowErr != nil && (errors.Is(workflowErr, context.Canceled) || errors.Is(workflowErr, context.DeadlineExceeded) || ctx.Err() != nil) {
		return nil, workflowErr
	}
	return s.buildResponse(descriptor.Workflow, run, descriptor.StartedAt, result, workflowErr)
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
		TemporalUIURL: s.temporalRunURL(run.GetID(), run.GetRunID()),
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

	failure := decodeWorkflowFailure(workflowErr, result)
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

// decodeWorkflowFailure reads the product failure contract, and any partial result the
// Workflow attached alongside it, from one error. A Workflow attaches the failure first
// and the result second: the SDK leaves surplus decode targets untouched and still reports
// success, so the order is the contract and cannot be inferred from what arrives.
func decodeWorkflowFailure(err error, result proto.Message) *orchestrationv1.WorkflowFailure {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		var failure orchestrationv1.WorkflowFailure
		if appErr.HasDetails() && appErr.Details(&failure, result) == nil && failure.Code != "" {
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

func (s *server) temporalRunURL(workflowID, runID string) string {
	return fmt.Sprintf("%s/namespaces/%s/workflows/%s/%s/history", s.temporalUI, url.PathEscape(s.namespace), url.PathEscape(workflowID), url.PathEscape(runID))
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
