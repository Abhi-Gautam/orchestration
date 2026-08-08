# Architecture

> **Status:** Current implementation

The platform separates product traffic from durable execution. The web process starts and observes operations; the Worker executes compiled Workflows and Activities.

## Components

| Component | Responsibility |
|---|---|
| Browser | Select Workflows, submit protobuf JSON, reconcile run snapshots |
| `cmd/web` | Catalog, start API, Temporal client, status Queries, History watchers, SSE |
| `cmd/worker` | Workflow and Activity registration, task-queue polling |
| Temporal | Durable execution, retries, timers, Queries, and Event History |
| PostgreSQL | Temporal persistence only |
| Temporal UI | Operator inspection, not the product UI |

The web and Worker must use the same protobuf contracts, Workflow catalog, namespace, and task queue. Only the Worker links executable Workflows and Activities.

## Run lifecycle

1. `cmd/web` builds its catalog from `workflowcatalog.Definitions()`.
2. The start API decodes input into the registered protobuf request type.
3. Web starts the registered Temporal Workflow and immediately returns its Workflow ID and Run ID.
4. `cmd/worker` polls the shared task queue and executes the Workflow.
5. The Workflow exposes product state through the `operation-status` Query.
6. Web long-polls History. New Workflow Task completions wake a status Query.
7. Newer status revisions are published as complete SSE snapshots.
8. When execution closes, web decodes the registered result or a structured failure.

Raw History is never sent to the product UI. It is an operator record and a wake-up mechanism, not a frontend event contract.

## Product state

A Workflow status contains its state, phase, friendly step, message, progress, revision, and currently available actions. The Workflow owns those values because action availability changes with runtime state.

Registry entries describe how to start and decode a Workflow. They must not duplicate runtime behavior with flags such as `CanPause` or `CanRetry`. See `design-rationale.md` for why static capability flags were rejected.

## Durability boundaries

| State | Current owner |
|---|---|
| Workflow state and execution History | Temporal |
| Temporal persistence | PostgreSQL |
| Active History watchers | Web process memory |
| Active browser run descriptors | Browser `sessionStorage` |
| Product run history | Not implemented |
| Reusable artifacts | RustFS object storage |
| Durable report records | SQLite application database |

Restarting web does not stop a Workflow, but clients must reattach to monitor it. Clearing browser session state removes the browser's active-run list, not the Temporal execution.

## Current constraints

- Workflows and Activities are compiled into this module.
- A dependency-light catalog supplies shared start contracts; the Worker separately maps Temporal names to executable Workflow functions.
- Activities are registered explicitly in `cmd/worker/main.go`.
- The provided stack uses one task queue and one Worker process.
- Authentication, tenant isolation, durable product history, and production deployment are not implemented.

## Code map

- Runtime wiring: `cmd/web/server.go`, `cmd/worker/main.go`
- Catalog and start contracts: `internal/workflowcatalog/catalog.go`
- Executable Workflow mapping: `internal/workflows/registry.go`
- Product status: `internal/workflows/status.go`
- Monitoring: `cmd/web/monitor.go`, `cmd/web/events.go`
- Contracts: `api/orchestration/v1/workflows.proto`
- Local stack: `compose.yaml`
