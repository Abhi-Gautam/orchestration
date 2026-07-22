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
- AI agent loops and comparison with LangGraph and LangChain
- User notifications and product-facing workflow state
- Product UI boundaries versus Temporal UI and operator details

Each topic is handled through four stages:

1. Research the problem, industry patterns, Temporal semantics, and tradeoffs.
2. Agree on a concrete example and its expected behavior.
3. Implement and verify it against the running Temporal stack.
4. Review the code and execution history to understand why the pattern works.

Topic notes and experiments live under `docs/topics/`.

## Local setup

The development stack uses official images:

- Temporal Server: `temporalio/auto-setup:1.29.2`
- Temporal UI: `temporalio/ui:2.44.1`
- PostgreSQL: `postgres:14`

The project uses `localhost:7234` for the Temporal frontend and [http://localhost:8234](http://localhost:8234) for Temporal UI because the default ports are already used by another local environment.

Start the stack:

```sh
docker compose up -d
```

Run the worker:

```sh
go run ./cmd/worker
```

Run the example from another terminal:

```sh
go run ./cmd/client
```

Stop the stack while preserving workflow history:

```sh
docker compose down
```

Reset the complete local environment:

```sh
docker compose down -v
```
