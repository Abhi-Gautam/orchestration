package main

import (
	"embed"
	"fmt"
	"io/fs"
	"net/http"

	"go.temporal.io/sdk/client"
)

//go:embed templates/*.gohtml static/*
var content embed.FS

type server struct {
	temporal   client.Client
	taskQueue  string
	temporalUI string
	namespace  string
	templates  *templates
	static     http.Handler
	monitor    *runMonitor
}

func newServer(temporalClient client.Client, cfg config) (*server, error) {
	tmpl, err := loadTemplates(content)
	if err != nil {
		return nil, err
	}

	staticRoot, err := fs.Sub(content, "static")
	if err != nil {
		return nil, fmt.Errorf("static files: %w", err)
	}

	server := &server{
		temporal:   temporalClient,
		taskQueue:  cfg.taskQueue,
		temporalUI: cfg.temporalUI,
		namespace:  cfg.temporalNamespace,
		templates:  tmpl,
		static:     http.FileServer(http.FS(staticRoot)),
	}
	server.monitor = newRunMonitor(server)
	return server, nil
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()

	// Server-rendered UI (HTMX partials + Alpine page).
	mux.HandleFunc("GET /{$}", s.handleIndex)
	mux.HandleFunc("GET /workflows", s.handleWorkflowListPartial)
	mux.HandleFunc("POST /runs", s.handleRunHTML)

	// JSON API kept for tooling and Alpine catalog refresh.
	mux.HandleFunc("GET /api/workflows", s.handleListWorkflows)
	mux.HandleFunc("POST /api/workflows/run", s.handleRunWorkflow)
	mux.HandleFunc("GET /api/runs/events", s.handleRunEvents)
	mux.HandleFunc("POST /api/runs/cancel", s.handleCancelRun)

	mux.Handle("GET /static/", http.StripPrefix("/static/", s.static))
	return mux
}
