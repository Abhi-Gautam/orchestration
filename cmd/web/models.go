package main

import (
	"encoding/json"
	"fmt"
	"time"
)

type catalogWorkflow struct {
	ID           string         `json:"id"`
	Name         string         `json:"name"`
	Description  string         `json:"description"`
	ExampleInput map[string]any `json:"exampleInput"`
}

type catalogResponse struct {
	Workflows []catalogWorkflow `json:"workflows"`
}
type runRequest struct {
	Workflow string          `json:"workflow"`
	Input    json.RawMessage `json:"input"`
}
type runResponse struct {
	Workflow      string    `json:"workflow"`
	Status        string    `json:"status"`
	WorkflowID    string    `json:"workflowId"`
	RunID         string    `json:"runId"`
	StartedAt     time.Time `json:"startedAt"`
	FinishedAt    time.Time `json:"finishedAt"`
	Elapsed       string    `json:"elapsed"`
	TemporalUIURL string    `json:"temporalUiUrl"`
	Result        any       `json:"result,omitempty"`
	Failure       *failure  `json:"failure,omitempty"`
}
type failure struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}
type errorResponse struct {
	Error string `json:"error"`
}
type greetingWebInput struct {
	Name string `json:"name"`
}
type simpleDiamondWebInput struct {
	PrepareDuration  durationValue `json:"prepareDuration"`
	BranchADuration  durationValue `json:"branchADuration"`
	BranchBDuration  durationValue `json:"branchBDuration"`
	FinalizeDuration durationValue `json:"finalizeDuration"`
}
type dynamicFanOutWebInput struct {
	RequestedCount   int           `json:"requestedCount"`
	BranchDuration   durationValue `json:"branchDuration"`
	FinalizeDuration durationValue `json:"finalizeDuration"`
}
type fanOutPolicyWebInput struct {
	Policy           string        `json:"policy"`
	BranchCount      int           `json:"branchCount"`
	FailureBranch    *int          `json:"failureBranch"`
	FaultMode        string        `json:"faultMode"`
	FinalizeDuration durationValue `json:"finalizeDuration"`
	BranchDuration   durationValue `json:"branchDuration"`
}

type durationValue time.Duration

func (d *durationValue) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*d = 0
		return nil
	}
	var text string
	if json.Unmarshal(data, &text) == nil {
		if text == "" {
			*d = 0
			return nil
		}
		parsed, err := time.ParseDuration(text)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", text, err)
		}
		*d = durationValue(parsed)
		return nil
	}
	var number int64
	if json.Unmarshal(data, &number) == nil {
		*d = durationValue(time.Duration(number))
		return nil
	}
	return fmt.Errorf("duration must be a string like \"1s\" or an integer number of nanoseconds")
}
func (d durationValue) Duration() time.Duration { return time.Duration(d) }
