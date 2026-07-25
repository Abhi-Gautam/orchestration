package main

import (
	"log"
	"net/http"
	"time"

	"github.com/joho/godotenv"
	"go.temporal.io/sdk/client"
)

func main() {
	_ = godotenv.Load()

	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.temporalAddress,
		Namespace: cfg.temporalNamespace,
	})
	if err != nil {
		log.Fatalf("connect to Temporal frontend: %v", err)
	}
	defer temporalClient.Close()

	app, err := newServer(temporalClient, cfg)
	if err != nil {
		log.Fatalf("create web server: %v", err)
	}

	server := &http.Server{
		Addr:              cfg.webAddress,
		Handler:           app.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("web server listening on %s (task queue %q, temporal %s)", cfg.webAddress, cfg.taskQueue, cfg.temporalAddress)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
