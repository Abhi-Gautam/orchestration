package main

import (
	"encoding/json"
	"time"
)

type catalogWorkflow struct {
	ID           string          `json:"id"`
	Name         string          `json:"name"`
	Description  string          `json:"description"`
	ExampleInput json.RawMessage `json:"exampleInput"`
}

type catalogResponse struct {
	Workflows []catalogWorkflow `json:"workflows"`
}

type runRequest struct {
	Workflow string          `json:"workflow"`
	Input    json.RawMessage `json:"input"`
}

type runResponse struct {
	Workflow      string          `json:"workflow"`
	Status        string          `json:"status"`
	WorkflowID    string          `json:"workflowId"`
	RunID         string          `json:"runId"`
	StartedAt     time.Time       `json:"startedAt"`
	FinishedAt    time.Time       `json:"finishedAt"`
	Elapsed       string          `json:"elapsed"`
	TemporalUIURL string          `json:"temporalUiUrl"`
	Result        json.RawMessage `json:"result,omitempty"`
	Failure       json.RawMessage `json:"failure,omitempty"`
}

type errorResponse struct {
	Error string `json:"error"`
}
