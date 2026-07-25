package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io/fs"
	"strings"
	"time"
)

type templates struct {
	page         *template.Template
	workflowList *template.Template
	runCard      *template.Template
	runError     *template.Template
}

type pageView struct {
	Workflows   []catalogWorkflow
	CatalogJSON template.JS
}

type runCardView struct {
	Status        string
	StatusKind    string
	Message       string
	Elapsed       string
	Workflow      string
	WorkflowID    string
	RunID         string
	StartedAt     string
	FinishedAt    string
	TemporalUIURL string
	OutputHeading string
	OutputPretty  string
	HasOutput     bool
	HasTemporalUI bool
}

type runErrorView struct {
	Message string
}

func loadTemplates(files fs.FS) (*templates, error) {
	page, err := template.ParseFS(files, "templates/page.gohtml")
	if err != nil {
		return nil, fmt.Errorf("parse page template: %w", err)
	}
	workflowList, err := template.ParseFS(files, "templates/workflow_list.gohtml")
	if err != nil {
		return nil, fmt.Errorf("parse workflow list template: %w", err)
	}
	runCard, err := template.ParseFS(files, "templates/run_card.gohtml")
	if err != nil {
		return nil, fmt.Errorf("parse run card template: %w", err)
	}
	runError, err := template.ParseFS(files, "templates/run_error.gohtml")
	if err != nil {
		return nil, fmt.Errorf("parse run error template: %w", err)
	}

	return &templates{
		page:         page,
		workflowList: workflowList,
		runCard:      runCard,
		runError:     runError,
	}, nil
}

func buildPageView(workflows []catalogWorkflow) (pageView, error) {
	catalogJSON, err := json.Marshal(workflows)
	if err != nil {
		return pageView{}, err
	}
	return pageView{
		Workflows:   workflows,
		CatalogJSON: template.JS(catalogJSON),
	}, nil
}

func buildRunCardView(response *runResponse) runCardView {
	kind := statusKind(response.Status)
	view := runCardView{
		Status:        humanizeStatus(response.Status),
		StatusKind:    kind,
		Message:       statusMessage(kind),
		Elapsed:       response.Elapsed,
		Workflow:      response.Workflow,
		WorkflowID:    response.WorkflowID,
		RunID:         response.RunID,
		StartedAt:     formatTime(response.StartedAt),
		FinishedAt:    formatTime(response.FinishedAt),
		TemporalUIURL: response.TemporalUIURL,
		HasTemporalUI: strings.TrimSpace(response.TemporalUIURL) != "",
	}

	if len(response.Failure) > 0 && string(response.Failure) != "null" {
		view.HasOutput = true
		view.OutputHeading = "Failure"
		view.OutputPretty = prettyJSON(response.Failure)
		return view
	}
	if len(response.Result) > 0 && string(response.Result) != "null" {
		view.HasOutput = true
		view.OutputHeading = "Result"
		view.OutputPretty = prettyJSON(response.Result)
	}
	return view
}

func prettyJSON(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return string(raw)
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return string(raw)
	}
	return string(encoded)
}

func statusKind(status string) string {
	normalized := strings.ToLower(status)
	switch {
	case strings.Contains(normalized, "fail"),
		strings.Contains(normalized, "error"),
		strings.Contains(normalized, "cancel"),
		strings.Contains(normalized, "terminate"),
		strings.Contains(normalized, "timeout"):
		return "failure"
	case strings.Contains(normalized, "complete"),
		strings.Contains(normalized, "success"),
		strings.Contains(normalized, "succeed"):
		return "success"
	default:
		return "warning"
	}
}

func statusMessage(kind string) string {
	switch kind {
	case "success":
		return "The workflow completed successfully."
	case "failure":
		return "The workflow did not complete successfully. Review the failure details below."
	default:
		return "The workflow finished with the status reported below."
	}
}

func humanizeStatus(status string) string {
	status = strings.TrimSpace(status)
	if status == "" {
		return "Unknown"
	}
	status = strings.ReplaceAll(status, "_", " ")
	status = strings.ReplaceAll(status, "-", " ")
	parts := strings.Fields(status)
	for i, part := range parts {
		if part == "" {
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + strings.ToLower(part[1:])
	}
	return strings.Join(parts, " ")
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return "—"
	}
	return value.Local().Format("Jan 2, 2006, 3:04:05 PM")
}
