package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io/fs"
	"time"
)

type templates struct {
	page         *template.Template
	workflowList *template.Template
	runPending   *template.Template
	runError     *template.Template
}

type pageView struct {
	Workflows   []catalogWorkflow
	CatalogJSON template.JS
}

type runPendingView struct {
	WorkflowName  string
	Workflow      string
	WorkflowID    string
	RunID         string
	StartedAt     string
	TemporalUIURL string
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
	runPending, err := template.ParseFS(files, "templates/run_pending.gohtml")
	if err != nil {
		return nil, fmt.Errorf("parse pending run template: %w", err)
	}
	runError, err := template.ParseFS(files, "templates/run_error.gohtml")
	if err != nil {
		return nil, fmt.Errorf("parse run error template: %w", err)
	}

	return &templates{
		page:         page,
		workflowList: workflowList,
		runPending:   runPending,
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

func buildRunPendingView(descriptor *runDescriptor) runPendingView {
	return runPendingView{
		WorkflowName:  descriptor.WorkflowName,
		Workflow:      descriptor.Workflow,
		WorkflowID:    descriptor.WorkflowID,
		RunID:         descriptor.RunID,
		StartedAt:     formatTime(descriptor.StartedAt),
		TemporalUIURL: descriptor.TemporalUIURL,
	}
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return "—"
	}
	return value.Local().Format("Jan 2, 2006, 3:04:05 PM")
}
