package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"orchestration/internal/workflowcatalog"
)

const maxEventRuns = 32

func (s *server) handleRunEvents(w http.ResponseWriter, r *http.Request) {
	descriptors, err := s.eventRunDescriptors(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "Streaming is not supported by this server.")
		return
	}

	subscriber := newRunSubscriber()
	unsubscribes := make([]func(), 0, len(descriptors))
	for _, descriptor := range descriptors {
		unsubscribe, err := s.monitor.subscribe(descriptor, subscriber)
		if err != nil {
			for _, unsubscribe := range unsubscribes {
				unsubscribe()
			}
			writeError(w, http.StatusBadRequest, "Conflicting workflow descriptor for run.")
			return
		}
		unsubscribes = append(unsubscribes, unsubscribe)
	}
	defer func() {
		for _, unsubscribe := range unsubscribes {
			unsubscribe()
		}
	}()

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if _, err := fmt.Fprint(w, ": connected\n\n"); err != nil {
		return
	}
	flusher.Flush()

	terminal := make(map[runKey]struct{}, len(descriptors))
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepalive.C:
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-subscriber.wake:
			monitorFailed := false
			for _, event := range subscriber.takeLatest() {
				if err := writeRunSSE(w, event); err != nil {
					return
				}
				key := runKey{workflowID: event.WorkflowID, runID: event.RunID}
				switch event.kind {
				case runEventTerminal:
					terminal[key] = struct{}{}
				case runEventMonitorError:
					monitorFailed = true
				}
			}
			flusher.Flush()
			if monitorFailed || len(terminal) == len(descriptors) {
				return
			}
		}
	}
}

func (s *server) eventRunDescriptors(r *http.Request) ([]runDescriptor, error) {
	values := r.URL.Query()["run"]
	if len(values) == 0 {
		return nil, fmt.Errorf("At least one run descriptor is required.")
	}
	if len(values) > maxEventRuns {
		return nil, fmt.Errorf("A maximum of %d runs may be monitored per stream.", maxEventRuns)
	}

	descriptors := make([]runDescriptor, 0, len(values))
	seen := make(map[runKey]string, len(values))
	for _, value := range values {
		var descriptor runDescriptor
		decoder := json.NewDecoder(strings.NewReader(value))
		decoder.DisallowUnknownFields()
		if err := decodeOne(decoder, &descriptor); err != nil {
			return nil, fmt.Errorf("Invalid run descriptor.")
		}

		descriptor.Workflow = strings.TrimSpace(descriptor.Workflow)
		definition, ok := workflowcatalog.FindDefinition(descriptor.Workflow)
		if !ok {
			return nil, fmt.Errorf("Unknown workflow id %q.", descriptor.Workflow)
		}
		if !validTemporalID(descriptor.WorkflowID) || !validTemporalID(descriptor.RunID) {
			return nil, fmt.Errorf("Invalid workflow or run id.")
		}

		descriptor.WorkflowName = definition.Name
		descriptor.Status = "running"
		descriptor.TemporalUIURL = s.temporalRunURL(descriptor.WorkflowID, descriptor.RunID)
		key := runKey{workflowID: descriptor.WorkflowID, runID: descriptor.RunID}
		if workflow, duplicate := seen[key]; duplicate {
			if workflow != descriptor.Workflow {
				return nil, fmt.Errorf("Conflicting workflow descriptors for run.")
			}
			continue
		}
		seen[key] = descriptor.Workflow
		descriptors = append(descriptors, descriptor)
	}
	return descriptors, nil
}

func validTemporalID(value string) bool {
	if value == "" || len(value) > 255 || strings.TrimSpace(value) != value || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func writeRunSSE(w http.ResponseWriter, event runEvent) error {
	encoded, err := json.Marshal(event)
	if err != nil {
		return err
	}
	eventName := "run"
	if event.kind == runEventMonitorError {
		eventName = "monitorError"
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", eventName, encoded)
	return err
}
