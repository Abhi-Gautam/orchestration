package workflows

import (
	"go.temporal.io/sdk/temporal"

	orchestrationv1 "orchestration/gen/orchestration/v1"
)

func invalidRequest(code, message string) error {
	failure := &orchestrationv1.WorkflowFailure{
		Code:      code,
		Message:   message,
		Category:  orchestrationv1.FailureCategory_FAILURE_CATEGORY_VALIDATION,
		Retryable: false,
	}
	return temporal.NewNonRetryableApplicationError(message, code, nil, failure)
}
