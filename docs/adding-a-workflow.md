# Adding a Workflow

> **Status:** Current source-extension process

Workflow start contracts are compiled into both web and Worker binaries. Executable Workflow and Activity implementations are linked only into the Worker. There is no runtime plugin loader or external catalog.

## 1. Define the contract

Add top-level request and result messages to `api/orchestration/v1/workflows.proto`, then regenerate Go code:

```sh
./scripts/generate-proto.sh
```

Preserve protobuf field numbers. Reserve removed fields and names. Top-level requests, results, status, and structured failures use protobuf; Activity-local types may be ordinary Go types.

## 2. Implement the Workflow

Add the Workflow under `internal/workflows/`.

- Validate business input before scheduling work.
- Keep Workflow code deterministic.
- Put network, filesystem, database, and other external I/O in Activities.
- Use Temporal Workflow time and timers, not `time.Now` or `time.Sleep`.
- Set explicit Activity timeouts, retries, and cancellation behavior.
- Install `newStatusTracker` when the operation needs live product status.
- Return compact results or durable references.

Validation failures should use `invalidRequest`, which returns a non-retryable Application Error carrying `WorkflowFailure` details.

## 3. Implement and register Activities

Add Activities under `internal/activities/`, then register each function in `cmd/worker/main.go`.

Activities may execute more than once. External side effects must be idempotent, deduplicated by the destination, or safely reusable. Heartbeat long-running Activities so cancellation and liveness are observable.

## 4. Register the Workflow

Add one `Definition` in `internal/workflowcatalog/catalog.go` with:

- A stable product ID.
- Display name and description.
- A stable Temporal Workflow type name.

- Request and result constructors.
- A valid example request.

Map the definition's Temporal name to its executable Workflow function in `internal/workflows/registry.go`. Worker startup rejects missing or extra implementations. The shared catalog drives the web catalog, input decoding, result decoding, Workflow-ID resolution, and monitoring validation. A generic Workflow does not need a new HTTP handler or page.

## 5. Verify it

```sh
go test ./...
docker compose up --build
```

Run the Workflow through the generic UI or API. Check the product result, structured failure paths, status revisions, Activity attempts, and Temporal History.

Before changing a deployed Workflow, consider replay compatibility. Worker Build ID routing and a release compatibility policy are not configured yet.

## When UI work is required

The generic UI can edit protobuf JSON and render generic results. Add UI code only when the Workflow needs a domain-specific form, action, or result view.

## Code map

- Contracts: `api/orchestration/v1/workflows.proto`
- Workflows: `internal/workflows/`
- Activities: `internal/activities/`
- Catalog and start contracts: `internal/workflowcatalog/catalog.go`
- Executable Workflow mapping: `internal/workflows/registry.go`
- Worker registration: `cmd/worker/main.go`
