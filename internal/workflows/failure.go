package workflows

import (
	"errors"

	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"

	orchestrationv1 "orchestration/gen/orchestration/v1"
)

// workflowFailure builds the terminal error a Workflow raises.
//
// The Temporal error is always non-retryable: a Workflow that has decided it failed should
// not be replayed as a new attempt. WorkflowFailure.Retryable answers a different, product
// question — whether starting the operation again could succeed. The two are named alike
// and mean different things, so they are set in one place rather than at each raise site.
//
// The failure is the first detail and a partial result, when one survives, the second. The
// SDK leaves surplus decode targets untouched and still reports success, so that order is
// the contract between a Workflow and the client decoding its failure.
func workflowFailure(failure *orchestrationv1.WorkflowFailure, partialResult proto.Message) error {
	details := []any{failure}
	if partialResult != nil {
		details = append(details, partialResult)
	}
	return temporal.NewNonRetryableApplicationError(failure.Message, failure.Code, nil, details...)
}

func invalidRequest(code, message string) error {
	return workflowFailure(&orchestrationv1.WorkflowFailure{
		Code:      code,
		Message:   message,
		Category:  orchestrationv1.FailureCategory_FAILURE_CATEGORY_VALIDATION,
		Retryable: false,
	}, nil)
}

// causeMetadata records the Temporal error type behind a failure without leaking the
// underlying error text into the product contract.
func causeMetadata(stage string, err error) map[string]string {
	metadata := map[string]string{"stage": stage}
	var applicationErr *temporal.ApplicationError
	if errors.As(err, &applicationErr) {
		metadata["temporalType"] = applicationErr.Type()
	}
	return metadata
}
