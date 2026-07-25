package main

import (
	"embed"
	"io/fs"
	"net/http"

	"go.temporal.io/sdk/client"
)

//go:embed static/*
var staticFiles embed.FS

type server struct {
	temporal   client.Client
	taskQueue  string
	temporalUI string
	namespace  string
	static     http.Handler
}

func newServer(temporalClient client.Client, cfg config) (*server, error) {
	staticRoot, err := fs.Sub(staticFiles, "static")
	if err != nil {
		return nil, err
	}

	return &server{
		temporal:   temporalClient,
		taskQueue:  cfg.taskQueue,
		temporalUI: cfg.temporalUI,
		namespace:  cfg.temporalNamespace,
		static:     http.FileServer(http.FS(staticRoot)),
	}, nil
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/workflows", s.handleListWorkflows)
	mux.HandleFunc("POST /api/workflows/run", s.handleRunWorkflow)
	mux.HandleFunc("GET /{$}", s.handleIndex)
	mux.Handle("GET /", s.static)
	return mux
}
