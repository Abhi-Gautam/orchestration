package main

import (
	"testing"

	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"

	orchestrationv1 "orchestration/gen/orchestration/v1"
)

func TestExtractFailureResultDecodesStructuredFanOutDetails(t *testing.T) {
	failure := &orchestrationv1.WorkflowFailure{
		Code:     "FAN_OUT_AGGREGATE_FAILURE",
		Message:  "fan-out completed with failures",
		Category: orchestrationv1.FailureCategory_FAILURE_CATEGORY_BUSINESS,
	}
	want := &orchestrationv1.FanOutPolicyResult{
		Planned:   1000,
		Succeeded: 900,
		Failed:    100,
		Aggregate: &orchestrationv1.FanOutAggregate{
			Complete:                  true,
			ConsumedSuccessfulOutputs: 900,
			SemanticDigest:            "digest",
		},
	}
	err := temporal.NewNonRetryableApplicationError(
		failure.Message,
		"FanOutAggregateFailure",
		nil,
		failure,
		want,
	)
	err = roundTripTemporalFailure(err)

	got := &orchestrationv1.FanOutPolicyResult{}
	extractFailureResult(err, got)
	if !proto.Equal(got, want) {
		t.Fatalf("decoded result = %v, want %v", got, want)
	}
}

func TestExtractFailureResultSupportsResultOnlyDetails(t *testing.T) {
	want := &orchestrationv1.FanOutPolicyResult{Planned: 6, Failed: 1}
	err := temporal.NewNonRetryableApplicationError("failed", "LegacyFanOutFailure", nil, want)
	err = roundTripTemporalFailure(err)

	got := &orchestrationv1.FanOutPolicyResult{}
	extractFailureResult(err, got)
	if !proto.Equal(got, want) {
		t.Fatalf("decoded result = %v, want %v", got, want)
	}
}

func roundTripTemporalFailure(err error) error {
	converter := temporal.GetDefaultFailureConverter()
	return converter.FailureToError(converter.ErrorToFailure(err))
}
