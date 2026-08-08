package workflows

import (
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orchestrationv1 "orchestration/gen/orchestration/v1"
	"orchestration/internal/activities"
)

func duration(value *durationpb.Duration) time.Duration {
	if value == nil {
		return 0
	}
	return value.AsDuration()
}

func itoa(value int32) string {
	return strconv.Itoa(int(value))
}

func waitResult(result activities.WaitResult) *orchestrationv1.WaitResult {
	return &orchestrationv1.WaitResult{
		Name:       result.Name,
		Attempt:    result.Attempt,
		StartedAt:  timestamppb.New(result.StartedAt),
		FinishedAt: timestamppb.New(result.FinishedAt),
		Elapsed:    durationpb.New(result.Elapsed),
	}
}

func waitResults(results []activities.WaitResult) []*orchestrationv1.WaitResult {
	converted := make([]*orchestrationv1.WaitResult, len(results))
	for i, result := range results {
		converted[i] = waitResult(result)
	}
	return converted
}
