# HTTP API

> **Status:** Current implementation; not yet a versioned public API

The web process exposes a generic catalog, asynchronous Workflow starts, and multiplexed SSE monitoring.

## Endpoints

| Method and path | Purpose |
|---|---|
| `GET /` | Embedded product UI |
| `GET /workflows` | Server-rendered catalog fragment |
| `POST /runs` | HTML form start |
| `GET /api/workflows` | JSON catalog |
| `POST /api/workflows/run` | JSON start |
| `GET /api/runs/events` | Monitor up to 32 runs over SSE |
| `GET /static/*` | Embedded static assets |

## Start a Workflow

```http
POST /api/workflows/run
Content-Type: application/json

{
  "workflow": "greeting",
  "input": {
    "name": "Temporal"
  }
}
```

A successful start returns `202 Accepted`:

```json
{
  "workflow": "greeting",
  "workflowName": "Greeting",
  "status": "running",
  "workflowId": "greeting-...",
  "runId": "...",
  "startedAt": "...",
  "temporalUiUrl": "http://localhost:8234/..."
}
```

The body limit is 1 MiB. Unknown top-level fields and multiple JSON values are rejected.

`input` uses protobuf JSON: field names are lower camel case, enums use symbolic names, durations and timestamps use protobuf formats, and 64-bit integers are JSON strings. Unknown protobuf fields are rejected.

## Monitor runs

Pass each returned run descriptor as a URL-encoded `run` query parameter:

```text
GET /api/runs/events?run=<descriptor>&run=<descriptor>
```

The stream accepts 1–32 runs and sends a keepalive comment every 15 seconds.

### `run`

Carries either:

- `operationStatus`: the latest complete status snapshot, or
- `runResponse`: the terminal result or structured failure.

Slow clients receive the newest snapshot per run instead of an unbounded queue. Status revisions let clients ignore stale snapshots.

### `monitorError`

Monitoring stopped unexpectedly. The client should reconnect with the same descriptor. A monitoring failure does not imply the Temporal Workflow failed.

The stream closes after every attached run is terminal or after a monitor error. It has no SSE event IDs or durable replay log.

## Terminal responses

Successful executions use status `completed` and contain `result`. Unsuccessful executions use status `failed` and contain a protobuf `WorkflowFailure`; some policies also preserve a partial `result`.

Cancellation is represented as a `CANCELED` structured failure inside an outer `failed` run response.

## Current limits

The API has no version prefix, authentication, authorization, tenant isolation, rate limiting, CORS policy, durable run listing, or control endpoints. Deploy it only in a trusted local environment until those boundaries are added.

## Code map

- Routes and handlers: `cmd/web/server.go`, `cmd/web/handlers.go`
- Models: `cmd/web/models.go`
- Start and result decoding: `cmd/web/runner.go`
- SSE: `cmd/web/events.go`, `cmd/web/monitor.go`
