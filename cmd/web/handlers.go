package main

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"

	"go.temporal.io/api/serviceerror"

	"orchestration/internal/workflowcatalog"
)

func (s *server) handleIndex(w http.ResponseWriter, _ *http.Request) {
	view, err := buildPageView(workflowCatalog())
	if err != nil {
		http.Error(w, "failed to render page", http.StatusInternalServerError)
		return
	}
	s.writeTemplate(w, http.StatusOK, "page.gohtml", s.templates.page, view)
}

func (s *server) handleWorkflowListPartial(w http.ResponseWriter, _ *http.Request) {
	s.writeTemplate(w, http.StatusOK, "workflow_list.gohtml", s.templates.workflowList, workflowCatalog())
}

func (s *server) handleRunHTML(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		s.writeTemplate(w, http.StatusBadRequest, "run_error.gohtml", s.templates.runError, runErrorView{
			Message: "Invalid form submission.",
		})
		return
	}

	workflowID := strings.TrimSpace(r.FormValue("workflow"))
	rawInput := strings.TrimSpace(r.FormValue("input"))
	if workflowID == "" {
		s.writeTemplate(w, http.StatusBadRequest, "run_error.gohtml", s.templates.runError, runErrorView{
			Message: "Missing workflow id.",
		})
		return
	}
	if rawInput == "" || rawInput == "null" {
		s.writeTemplate(w, http.StatusBadRequest, "run_error.gohtml", s.templates.runError, runErrorView{
			Message: "Missing workflow input.",
		})
		return
	}
	if !json.Valid([]byte(rawInput)) {
		s.writeTemplate(w, http.StatusBadRequest, "run_error.gohtml", s.templates.runError, runErrorView{
			Message: "The payload is not valid JSON.",
		})
		return
	}

	descriptor, err := s.startWorkflow(r.Context(), workflowID, json.RawMessage(rawInput))
	if err != nil {
		s.writeTemplate(w, runHTMLStatus(err), "run_error.gohtml", s.templates.runError, runErrorView{
			Message: publicRunError(err),
		})
		return
	}

	trigger, err := json.Marshal(map[string]any{"runStarted": descriptor})
	if err != nil {
		log.Printf("encode run started trigger: %v", err)
		s.writeTemplate(w, http.StatusInternalServerError, "run_error.gohtml", s.templates.runError, runErrorView{
			Message: "The workflow started, but its run metadata could not be returned.",
		})
		return
	}
	w.Header().Set("HX-Trigger-After-Swap", string(trigger))
	s.writeTemplate(w, http.StatusAccepted, "run_pending.gohtml", s.templates.runPending, buildRunPendingView(descriptor))
}

func (s *server) handleListWorkflows(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, catalogResponse{Workflows: workflowCatalog()})
}

func (s *server) handleRunWorkflow(w http.ResponseWriter, r *http.Request) {
	var req runRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decodeOne(decoder, &req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid JSON request body.")
		return
	}
	req.Workflow = strings.TrimSpace(req.Workflow)
	if req.Workflow == "" {
		writeError(w, http.StatusBadRequest, "Missing workflow id.")
		return
	}
	if len(req.Input) == 0 || string(req.Input) == "null" {
		writeError(w, http.StatusBadRequest, "Missing workflow input.")
		return
	}

	descriptor, err := s.startWorkflow(r.Context(), req.Workflow, req.Input)
	if err != nil {
		writeError(w, runHTMLStatus(err), publicRunError(err))
		return
	}
	writeJSON(w, http.StatusAccepted, descriptor)
}

// handleCancelRun asks Temporal to cancel a run. Temporal is the authority on whether the
// request is still valid: the action a Workflow advertises is a hint the caller may act on,
// and a run that closed in the meantime rejects it here rather than in the browser.
func (s *server) handleCancelRun(w http.ResponseWriter, r *http.Request) {
	var req cancelRunRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16))
	decoder.DisallowUnknownFields()
	if err := decodeOne(decoder, &req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid JSON request body.")
		return
	}
	if _, known := workflowcatalog.FindDefinition(strings.TrimSpace(req.Workflow)); !known {
		writeError(w, http.StatusBadRequest, "Unknown workflow id.")
		return
	}
	if !validTemporalID(req.WorkflowID) || !validTemporalID(req.RunID) {
		writeError(w, http.StatusBadRequest, "Invalid workflow or run id.")
		return
	}

	if err := s.temporal.CancelWorkflow(r.Context(), req.WorkflowID, req.RunID); err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			writeError(w, http.StatusConflict, "This run is no longer running.")
			return
		}
		log.Printf("cancel workflow %s run %s: %v", req.WorkflowID, req.RunID, err)
		writeError(w, http.StatusBadGateway, "The run could not be cancelled.")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "canceling"})
}

func (s *server) writeTemplate(w http.ResponseWriter, status int, name string, tmpl interface {
	ExecuteTemplate(w io.Writer, name string, data any) error
}, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if err := tmpl.ExecuteTemplate(w, name, data); err != nil {
		log.Printf("render template %s: %v", name, err)
	}
}

func runHTMLStatus(err error) int {
	var inputErr *inputError
	if errors.As(err, &inputErr) {
		return http.StatusBadRequest
	}
	var startErr *startError
	if errors.As(err, &startErr) {
		return http.StatusBadGateway
	}
	return http.StatusInternalServerError
}

func publicRunError(err error) string {
	var inputErr *inputError
	if errors.As(err, &inputErr) {
		return inputErr.Error()
	}
	var startErr *startError
	if errors.As(err, &startErr) {
		return startErr.Error()
	}
	log.Printf("run workflow: %v", err)
	return "Unexpected server failure while running the workflow."
}

func decodeOne(decoder *json.Decoder, dest any) error {
	if err := decoder.Decode(dest); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}

func sanitizeDecodeError(err error) string {
	msg := strings.TrimPrefix(err.Error(), "json: ")
	if len(msg) > 200 {
		return "could not decode the provided JSON object"
	}
	return msg
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("write json response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}
