package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"strings"
)

func (s *server) handleIndex(w http.ResponseWriter, _ *http.Request) {
	data, err := fs.ReadFile(staticFiles, "static/index.html")
	if err != nil {
		http.Error(w, "index not found", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
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
	if strings.TrimSpace(req.Workflow) == "" {
		writeError(w, http.StatusBadRequest, "Missing workflow id.")
		return
	}
	if len(req.Input) == 0 || string(req.Input) == "null" {
		writeError(w, http.StatusBadRequest, "Missing workflow input.")
		return
	}
	response, err := s.executeWorkflow(r.Context(), req.Workflow, req.Input)
	if err != nil {
		var inputErr *inputError
		if errors.As(err, &inputErr) {
			writeError(w, http.StatusBadRequest, inputErr.Error())
			return
		}
		var startErr *startError
		if errors.As(err, &startErr) {
			writeError(w, http.StatusBadGateway, startErr.Error())
			return
		}
		log.Printf("run workflow %q: %v", req.Workflow, err)
		writeError(w, http.StatusInternalServerError, "Unexpected server failure while running the workflow.")
		return
	}
	writeJSON(w, http.StatusOK, response)
}
func decodeInput(raw json.RawMessage, dest any) error {
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decodeOne(decoder, dest); err != nil {
		return &inputError{message: fmt.Sprintf("Invalid workflow input: %s", sanitizeDecodeError(err))}
	}
	return nil
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
func jsonHasField(raw json.RawMessage, field string) bool {
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil {
		return false
	}
	_, ok := object[field]
	return ok
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
