package workflows

import (
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	orchestrationv1 "orchestration/gen/orchestration/v1"
	"orchestration/internal/workflowcatalog"
)

// statusTracker owns the operation's product status. It counts work as it is scheduled and
// settles, and derives every published number from those counters, so a Workflow states
// what happened rather than recomputing in-flight and percentage arithmetic at each step.
type statusTracker struct {
	status    *orchestrationv1.OperationStatus
	planned   int64
	scheduled int64
	succeeded int64
	failed    int64
	canceled  int64
}

func newStatusTracker(ctx workflow.Context, planned int64, phase, currentStep, message string) (*statusTracker, error) {
	tracker := &statusTracker{planned: planned}
	tracker.status = &orchestrationv1.OperationStatus{
		State:            orchestrationv1.OperationState_OPERATION_STATE_RUNNING,
		Phase:            phase,
		CurrentStep:      currentStep,
		Message:          message,
		Revision:         1,
		Progress:         tracker.progress(),
		AvailableActions: []orchestrationv1.OperationAction{orchestrationv1.OperationAction_OPERATION_ACTION_CANCEL},
	}
	if err := workflow.SetQueryHandler(ctx, workflowcatalog.OperationStatusQueryName, func() (*orchestrationv1.OperationStatus, error) {
		return proto.Clone(tracker.status).(*orchestrationv1.OperationStatus), nil
	}); err != nil {
		return nil, err
	}
	return tracker, nil
}

// planWork revises the total once a Workflow learns it at runtime.
func (tracker *statusTracker) planWork(planned int64) { tracker.planned = planned }

func (tracker *statusTracker) scheduleWork(count int64) { tracker.scheduled += count }

func (tracker *statusTracker) recordSucceeded() { tracker.succeeded++ }

func (tracker *statusTracker) recordFailed() { tracker.failed++ }

func (tracker *statusTracker) recordCanceled() { tracker.canceled++ }

func (tracker *statusTracker) setRunning(phase, currentStep, message string) {
	tracker.publish(orchestrationv1.OperationState_OPERATION_STATE_RUNNING, phase, currentStep, message)
}

func (tracker *statusTracker) setSucceeded(phase, message string) {
	tracker.publish(orchestrationv1.OperationState_OPERATION_STATE_SUCCEEDED, phase, "", message)
}

func (tracker *statusTracker) setFailed(phase, message string) {
	tracker.publish(orchestrationv1.OperationState_OPERATION_STATE_FAILED, phase, "", message)
}

func (tracker *statusTracker) setCanceled(phase, message string) {
	tracker.publish(orchestrationv1.OperationState_OPERATION_STATE_CANCELED, phase, "", message)
}

func (tracker *statusTracker) publish(state orchestrationv1.OperationState, phase, currentStep, message string) {
	tracker.status.State = state
	tracker.status.Phase = phase
	tracker.status.CurrentStep = currentStep
	tracker.status.Message = message
	tracker.status.Progress = tracker.progress()
	tracker.status.Revision++
	tracker.status.AvailableActions = nil
	if state == orchestrationv1.OperationState_OPERATION_STATE_RUNNING {
		tracker.status.AvailableActions = []orchestrationv1.OperationAction{orchestrationv1.OperationAction_OPERATION_ACTION_CANCEL}
	}
}

func (tracker *statusTracker) progress() *orchestrationv1.OperationProgress {
	settled := tracker.succeeded + tracker.failed + tracker.canceled
	progress := &orchestrationv1.OperationProgress{
		Planned:    tracker.planned,
		Scheduled:  tracker.scheduled,
		InFlight:   max(tracker.scheduled-settled, 0),
		Succeeded:  tracker.succeeded,
		Failed:     tracker.failed,
		Canceled:   tracker.canceled,
		NotStarted: max(tracker.planned-tracker.scheduled, 0),
	}
	if tracker.planned > 0 {
		progress.Percent = min(float64(settled)*100/float64(tracker.planned), 100)
	}
	return progress
}
