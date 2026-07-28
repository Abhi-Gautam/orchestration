# Workflow engine patterns with Temporal

This repository explores the things a reliable workflow engine needs to get right. We are using Temporal and Go to research each topic, model an example, implement it, and verify the behavior.

## Topics

- Building static and runtime workflow DAGs
- Sequential dependencies and conditional branches
- Loops, polling, watchers, Signals, Updates, and timers
- Activity and Child Workflow fan-out/fan-in
- Concurrency limits, rate limits, fairness, and backpressure
- Pause, resume, retry, reset, cancel, and terminate behavior
- Failure propagation, fail-fast, best-effort, all-settled, and compensation policies
- Retry semantics and idempotency boundaries
- Workflow IDs, duplicate handling, shared work, and result coalescing
- Long-running Activities, heartbeats, worker failure, and recovery
- Progress reporting, metrics, tracing, memory use, and latency
- Workflow, Activity, replay, integration, and end-to-end testing
- Worker deployment, versioning, compatibility, and publishing new workflows
- Schedules, external events, API triggers, and transactional outbox patterns
- Dependencies between Workflows and Child Workflows
- Long-lived user sessions and entity workflows
- AI agent loops and comparison with LangGraph
- User notifications and product-facing workflow state
- Product UI boundaries versus Temporal UI and operator details

Topic notes and experiments live under `docs/topics/`.

## Architecture

There are exactly two application processes, plus Temporal from Compose:

The UI is plain HTML templates with **HTMX** (async workflow starts / partial swaps) and **Alpine.js** (selection state and live run reconciliation). Workflow starts return immediately, and one multiplexed **Server-Sent Events (SSE)** connection streams product-facing status snapshots for active runs.

The web container never runs a Temporal worker. The worker container is the only worker for this lab.

### Workflow contracts

Top-level Workflow requests, results, and structured failures are defined once in `api/orchestration/v1/workflows.proto`. Generated Go types live under `gen/orchestration/v1` and are used by both `cmd/web` and `internal/workflows`.

The web layer uses the registry in `internal/workflows/registry.go` to discover each Workflow's concrete request and result types. Adding a Workflow requires one schema definition, one implementation, and one registry entry; it does not require a new HTTP handler or new UI page. Activity timeouts, retries, and execution details remain owned by Workflow code.
