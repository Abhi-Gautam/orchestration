package workflows

import (
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	orchestrationv1 "orchestration/gen/orchestration/v1"
)

const OperationStatusQueryName = "operation-status"

type statusTracker struct {
	status *orchestrationv1.OperationStatus
}

func newStatusTracker(
	ctx workflow.Context,
	phase string,
	currentStep string,
	message string,
	progress *orchestrationv1.OperationProgress,
) (*statusTracker, error) {
	tracker := &statusTracker{status: &orchestrationv1.OperationStatus{
		State:            orchestrationv1.OperationState_OPERATION_STATE_RUNNING,
		Phase:            phase,
		CurrentStep:      currentStep,
		Message:          message,
		Revision:         1,
		Progress:         cloneProgress(progress),
		AvailableActions: []orchestrationv1.OperationAction{orchestrationv1.OperationAction_OPERATION_ACTION_CANCEL},
	}}
	if err := workflow.SetQueryHandler(ctx, OperationStatusQueryName, func() (*orchestrationv1.OperationStatus, error) {
		return proto.Clone(tracker.status).(*orchestrationv1.OperationStatus), nil
	}); err != nil {
		return nil, err
	}
	return tracker, nil
}

func (tracker *statusTracker) setRunning(phase, currentStep, message string, progress *orchestrationv1.OperationProgress) {
	tracker.status.State = orchestrationv1.OperationState_OPERATION_STATE_RUNNING
	tracker.status.Phase = phase
	tracker.status.CurrentStep = currentStep
	tracker.status.Message = message
	tracker.status.Progress = cloneProgress(progress)
	tracker.status.AvailableActions = []orchestrationv1.OperationAction{orchestrationv1.OperationAction_OPERATION_ACTION_CANCEL}
	tracker.status.Revision++
}

func (tracker *statusTracker) setSucceeded(phase, message string, progress *orchestrationv1.OperationProgress) {
	tracker.status.State = orchestrationv1.OperationState_OPERATION_STATE_SUCCEEDED
	tracker.status.Phase = phase
	tracker.status.CurrentStep = ""
	tracker.status.Message = message
	tracker.status.Progress = cloneProgress(progress)
	tracker.status.AvailableActions = nil
	tracker.status.Revision++
}

func operationProgress(planned, scheduled, inFlight, succeeded, failed, canceled int64) *orchestrationv1.OperationProgress {
	notStarted := planned - scheduled
	if notStarted < 0 {
		notStarted = 0
	}
	percent := float64(0)
	if planned > 0 {
		percent = float64(succeeded+failed+canceled) * 100 / float64(planned)
		if percent > 100 {
			percent = 100
		}
	}
	return &orchestrationv1.OperationProgress{
		Planned:    planned,
		Scheduled:  scheduled,
		InFlight:   inFlight,
		Succeeded:  succeeded,
		Failed:     failed,
		Canceled:   canceled,
		NotStarted: notStarted,
		Percent:    percent,
	}
}

func cloneProgress(progress *orchestrationv1.OperationProgress) *orchestrationv1.OperationProgress {
	if progress == nil {
		return nil
	}
	return proto.Clone(progress).(*orchestrationv1.OperationProgress)
}
