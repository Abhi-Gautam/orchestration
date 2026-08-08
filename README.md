# Orchestration

A Temporal-backed base for durable business operations and AI workflows. It provides compiled workflow discovery, asynchronous starts, product-facing status, SSE updates, structured failures, and a generic UI.

## Why this exists

Teams adopt a durable execution engine and then keep their own tables "just for the UI". A run row, a per-step status, a variables table, a dedup ledger, a progress feed. Each addition is reasonable on its own. Within a year, durability, retries, deduplication, cancellation, and progress each exist twice, and every hand-built copy is wrong in a different way. What is left is a database-backed workflow interpreter wearing a Temporal costume.

This repository is the counter-example. Each Workflow isolates one execution concern and runs against a real stack, so the rules below rest on evidence rather than assertion.

**North star:** a user or agent selects a Workflow, supplies input, and never needs to know its internal DAG. The Workflow contract owns operation identity, reusable work, valid commands, exposed progress, and whether regeneration is meaningful. Nothing about a running operation is duplicated outside Temporal.

The failure modes behind each rule, the experiment that answers each one, and the designs rejected along the way are in [Design rationale](docs/design-rationale.md).

This repository is currently a source-extension lab, not a runtime plugin platform. Adding a Workflow means changing and rebuilding this Go module.

## Run locally

Prerequisites: Docker with Compose.

```sh
cp .env.example .env
docker compose up --build
```

Open:

- Product UI: [http://localhost:8090](http://localhost:8090)
- Temporal UI: [http://localhost:8234](http://localhost:8234)

Select **Greeting**, keep the example input, and start the run. The product UI streams status while Temporal UI exposes the execution History.

Stop the stack with:

```sh
docker compose down
```

## Runtime

```mermaid
flowchart TD
    UI[Product UI] -->|HTTP and SSE| Web[Web process]
    Web -->|Start, Query, History| Temporal[Temporal]
    Worker[Worker process] -->|Poll task queue| Temporal
    Temporal --> DB[(PostgreSQL)]
    Worker --> Code[Compiled Workflows and Activities]
```

- `cmd/web` serves the UI and API. It is a Temporal client, not a Worker.
- `cmd/worker` registers and executes all Workflows and Activities.
- Temporal owns execution state and History.
- PostgreSQL stores Temporal state; there is no application run database yet.
- Protobuf defines top-level Workflow requests, results, status, and failures.

## Documentation

| Document | Use it when |
|---|---|
| [Design rationale](docs/design-rationale.md) | You want the failure modes this project answers, or you are about to reintroduce a rejected design |
| [Architecture](docs/architecture.md) | You need the runtime boundaries and execution lifecycle |
| [Adding a Workflow](docs/adding-a-workflow.md) | You are adding business Workflows and Activities |
| [Execution semantics](docs/execution-semantics.md) | You need branching, fan-out, failure, or control behavior |
| [Data and artifacts](docs/data-and-artifacts.md) | Work produces durable side effects or large outputs |
| [HTTP API](docs/http-api.md) | You are building another API client or UI |

## Current boundaries

The supplied Compose stack is for local development. The project does not yet provide authentication, tenant isolation, a public extension SDK, dynamic Workflow installation, production deployment manifests, or user-facing control APIs such as cancel and retry.

Execution concerns that remain unproven here, including run-tree cancellation, Workflow versioning and replay, and run attach, are listed in [Design rationale](docs/design-rationale.md#not-yet-answered).

Run the Go checks with:

```sh
go test ./...
```
