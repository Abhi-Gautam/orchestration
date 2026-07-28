package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/protobuf/encoding/protojson"

	orchestrationv1 "orchestration/gen/orchestration/v1"
	"orchestration/internal/workflows"
)

type runKey struct {
	workflowID string
	runID      string
}

type runMonitor struct {
	server *server

	mu       sync.Mutex
	watchers map[runKey]*runWatcher
}

func newRunMonitor(server *server) *runMonitor {
	return &runMonitor{
		server:   server,
		watchers: make(map[runKey]*runWatcher),
	}
}

func (monitor *runMonitor) subscribe(descriptor runDescriptor, subscriber *runSubscriber) (func(), error) {
	key := runKey{workflowID: descriptor.WorkflowID, runID: descriptor.RunID}

	monitor.mu.Lock()
	watcher := monitor.watchers[key]
	start := false
	if watcher == nil {
		// A watcher outlives any one SSE request while another subscriber remains attached.
		ctx, cancel := context.WithCancel(context.Background())
		watcher = &runWatcher{
			monitor:     monitor,
			key:         key,
			descriptor:  descriptor,
			ctx:         ctx,
			cancel:      cancel,
			subscribers: make(map[*runSubscriber]struct{}),
		}
		monitor.watchers[key] = watcher
		start = true
	}
	if err := watcher.addSubscriber(descriptor, subscriber); err != nil {
		monitor.mu.Unlock()
		return nil, err
	}
	monitor.mu.Unlock()

	if start {
		go watcher.run()
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			watcher.removeSubscriber(subscriber)
		})
	}, nil
}

func (monitor *runMonitor) unsubscribe(watcher *runWatcher, subscriber *runSubscriber) {
	monitor.mu.Lock()
	watcher.mu.Lock()
	delete(watcher.subscribers, subscriber)
	empty := len(watcher.subscribers) == 0
	if empty && monitor.watchers[watcher.key] == watcher {
		delete(monitor.watchers, watcher.key)
	}
	watcher.mu.Unlock()
	monitor.mu.Unlock()

	if empty {
		watcher.cancel()
	}
}

type runWatcher struct {
	monitor    *runMonitor
	key        runKey
	descriptor runDescriptor
	ctx        context.Context
	cancel     context.CancelFunc

	mu           sync.Mutex
	subscribers  map[*runSubscriber]struct{}
	latest       *runEvent
	lastRevision int64
}

func (watcher *runWatcher) addSubscriber(descriptor runDescriptor, subscriber *runSubscriber) error {
	watcher.mu.Lock()
	defer watcher.mu.Unlock()

	if watcher.descriptor.Workflow != descriptor.Workflow {
		return fmt.Errorf("run is already monitored as workflow %q", watcher.descriptor.Workflow)
	}
	watcher.subscribers[subscriber] = struct{}{}
	if watcher.latest != nil {
		subscriber.offer(watcher.key, *watcher.latest)
	}
	return nil
}

func (watcher *runWatcher) removeSubscriber(subscriber *runSubscriber) {
	watcher.monitor.unsubscribe(watcher, subscriber)
}

func (watcher *runWatcher) run() {
	describe, err := watcher.monitor.server.temporal.DescribeWorkflowExecution(
		watcher.ctx,
		watcher.descriptor.WorkflowID,
		watcher.descriptor.RunID,
	)
	if err != nil {
		watcher.fail(err)
		return
	}

	if describe == nil {
		watcher.fail(errors.New("describe workflow returned no response"))
		return
	}
	info := describe.GetWorkflowExecutionInfo()
	if info == nil {
		watcher.fail(errors.New("describe workflow returned no execution info"))
		return
	}
	definition, ok := workflows.FindDefinition(watcher.descriptor.Workflow)
	if !ok {
		watcher.fail(fmt.Errorf("unknown workflow %q", watcher.descriptor.Workflow))
		return
	}
	if workflowType := info.GetType().GetName(); workflowType != "" && workflowType != definition.TemporalName {
		watcher.fail(fmt.Errorf("workflow type %q does not match %q", workflowType, definition.TemporalName))
		return
	}

	descriptor := watcher.descriptor
	if startTime := info.GetStartTime(); startTime != nil {
		descriptor.StartedAt = startTime.AsTime()
		watcher.mu.Lock()
		watcher.descriptor.StartedAt = descriptor.StartedAt
		watcher.mu.Unlock()
	}

	if info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED {
		watcher.fail(errors.New("describe workflow returned an unspecified execution status"))
		return
	}
	if info.GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		watcher.finish(descriptor)
		return
	}

	watcher.queryStatus(descriptor)
	initialHistoryLength := info.GetHistoryLength()
	iterator := watcher.monitor.server.temporal.GetWorkflowHistory(
		watcher.ctx,
		descriptor.WorkflowID,
		descriptor.RunID,
		true,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
	)

	var skipped int64
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			watcher.fail(err)
			return
		}
		// Historical workflow-task completions predate this attachment and must not trigger queries.
		if skipped < initialHistoryLength {
			skipped++
			continue
		}
		if event.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			continue
		}
		watcher.queryStatus(descriptor)
	}

	if watcher.ctx.Err() != nil {
		return
	}
	watcher.finish(descriptor)
}

func (watcher *runWatcher) queryStatus(descriptor runDescriptor) {
	value, err := watcher.monitor.server.temporal.QueryWorkflow(
		watcher.ctx,
		descriptor.WorkflowID,
		descriptor.RunID,
		workflows.OperationStatusQueryName,
	)
	if err != nil {
		if watcher.ctx.Err() == nil {
			log.Printf("query operation status for workflow %s run %s: %v", descriptor.WorkflowID, descriptor.RunID, err)
		}
		return
	}

	var status orchestrationv1.OperationStatus
	if err := value.Get(&status); err != nil {
		log.Printf("decode operation status for workflow %s run %s: %v", descriptor.WorkflowID, descriptor.RunID, err)
		return
	}
	watcher.mu.Lock()
	if status.Revision <= watcher.lastRevision {
		watcher.mu.Unlock()
		return
	}
	watcher.lastRevision = status.Revision
	watcher.mu.Unlock()

	encoded, err := protojson.Marshal(&status)
	if err != nil {
		log.Printf("encode operation status for workflow %s run %s: %v", descriptor.WorkflowID, descriptor.RunID, err)
		return
	}

	watcher.publish(runEvent{
		Workflow:        descriptor.Workflow,
		WorkflowName:    descriptor.WorkflowName,
		WorkflowID:      descriptor.WorkflowID,
		RunID:           descriptor.RunID,
		OperationStatus: encoded,
		kind:            runEventStatus,
	})
}

func (watcher *runWatcher) finish(descriptor runDescriptor) {
	response, err := watcher.monitor.server.awaitWorkflow(watcher.ctx, descriptor)
	if err != nil {
		watcher.fail(err)
		return
	}
	watcher.publish(runEvent{
		Workflow:     descriptor.Workflow,
		WorkflowName: descriptor.WorkflowName,
		WorkflowID:   descriptor.WorkflowID,
		RunID:        descriptor.RunID,
		RunResponse:  response,
		kind:         runEventTerminal,
	})
}

func (watcher *runWatcher) fail(err error) {
	if watcher.ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	log.Printf("monitor workflow %s run %s: %v", watcher.key.workflowID, watcher.key.runID, err)
	watcher.publish(runEvent{
		Workflow:     watcher.descriptor.Workflow,
		WorkflowName: watcher.descriptor.WorkflowName,
		WorkflowID:   watcher.descriptor.WorkflowID,
		RunID:        watcher.descriptor.RunID,
		Error:        "Run monitoring was interrupted. Reattach to continue.",
		kind:         runEventMonitorError,
	})
}

func (watcher *runWatcher) publish(event runEvent) {
	watcher.mu.Lock()
	watcher.latest = &event
	for subscriber := range watcher.subscribers {
		subscriber.offer(watcher.key, event)
	}
	watcher.mu.Unlock()
}

type runSubscriber struct {
	mu     sync.Mutex
	latest map[runKey]runEvent
	wake   chan struct{}
}

func newRunSubscriber() *runSubscriber {
	return &runSubscriber{
		latest: make(map[runKey]runEvent),
		wake:   make(chan struct{}, 1),
	}
}

func (subscriber *runSubscriber) offer(key runKey, event runEvent) {
	// One map entry and one wake token bound slow subscribers while retaining each run's newest state.
	subscriber.mu.Lock()
	subscriber.latest[key] = event
	subscriber.mu.Unlock()

	select {
	case subscriber.wake <- struct{}{}:
	default:
	}
}

func (subscriber *runSubscriber) takeLatest() []runEvent {
	subscriber.mu.Lock()
	latest := subscriber.latest
	subscriber.latest = make(map[runKey]runEvent, len(latest))
	subscriber.mu.Unlock()

	events := make([]runEvent, 0, len(latest))
	for _, event := range latest {
		events = append(events, event)
	}
	return events
}
