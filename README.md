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

## Architecture

There are exactly two application processes, plus Temporal from Compose:

```text
Browser  →  cmd/web  →  Temporal Server  →  cmd/worker
              |               |                  |
     HTMX + Alpine       compose stack      only worker
     HTML partials                         (registers all
     + JSON API                            Workflows + Activities)
```

| Piece | Role |
| --- | --- |
| `compose.yaml` | PostgreSQL, Temporal Server, Temporal UI, **worker**, and **web** |
| `cmd/web` | Server-rendered UI (HTMX + Alpine) and Temporal client; no worker inside |
| `cmd/worker` | The only Temporal worker process |

The UI is plain HTML templates with **HTMX** (async form posts / partial swaps) and **Alpine.js** (selection state, concurrent in-flight run cards). Endpoints stay synchronous; the browser can run several `/runs` requests at once.

Shared task queue and local addresses are configured in `.env`.

The web container never runs a Temporal worker. The worker container is the only worker for this lab.

### Workflow contracts

Top-level Workflow requests, results, and structured failures are defined once in `api/orchestration/v1/workflows.proto`. Generated Go types live under `gen/orchestration/v1` and are used by both `cmd/web` and `internal/workflows`.

Regenerate them after editing the schema:

```sh
./scripts/generate-proto.sh
```

The web layer uses the registry in `internal/workflows/registry.go` to discover each Workflow's concrete request and result types. Adding a Workflow requires one schema definition, one implementation, and one registry entry; it does not require a new HTTP handler or new UI page. Activity timeouts, retries, and execution details remain owned by Workflow code.

Useful routes:

| Route | Purpose |
| --- | --- |
| `GET /` | Full page (templates + Alpine) |
| `POST /runs` | Start a Workflow and return an HTML result card (HTMX) |
| `GET /api/workflows` | JSON catalog |
| `POST /api/workflows/run` | JSON run (same execution path as `/runs`) |

Only run one local worker against the `orchestration` task queue. Old workers with stale Workflow signatures can consume tasks and produce payload-decoding failures.

## Port map (this project only)

These host ports are chosen so this lab can sit beside another Temporal install without sharing servers:

| Service | Host | Notes |
| --- | --- | --- |
| Product web UI | `http://localhost:8090` | Compose service `web` |
| Temporal frontend | `localhost:7234` | maps container `7233` |
| Temporal UI | `http://localhost:8234` | maps container `8080` |
| Compose project | `orchestration` | containers named `orchestration-*` |
| Docker network | `orchestration-network` | isolated from other stacks |
| Volume | `orchestration-temporal-postgres-data` | project-scoped |

Inside Compose, web and worker talk to Temporal at `temporal:7233`.
On the host (Air), `.env` uses `localhost:7234`.

A default Temporal stack elsewhere often uses `7233` and `8080`. This repo deliberately does not.

## Local setup

Create your local environment file once:

```sh
cp .env.example .env
```

Both Go commands load this file on startup. Docker Compose also uses it for variable interpolation.

### Start everything

```sh
docker compose up -d --build
```

That starts:

1. PostgreSQL
2. Temporal Server
3. Temporal UI
4. The only worker (`cmd/worker`)
5. The product web UI (`cmd/web`)

Open:

```text
http://localhost:8090
```

Temporal UI: [http://localhost:8234](http://localhost:8234)

Confirm the stack:

```sh
docker compose ps
```

You should see `orchestration-postgresql`, `orchestration-temporal`, `orchestration-temporal-ui`, `orchestration-worker`, and `orchestration-web`.

### Rebuild after code changes

```sh
docker compose up -d --build worker web
```

### Configuration

Local values live in `.env`; committed defaults are documented in `.env.example`.

| Variable | Purpose |
| --- | --- |
| `WEB_ADDRESS` | Product web server listen address |
| `TEMPORAL_ADDRESS` | Temporal frontend address |
| `TEMPORAL_UI_ADDRESS` | Browser-facing Temporal UI base URL |
| `TEMPORAL_NAMESPACE` | Temporal namespace used by both processes |
| `TASK_QUEUE` | Task queue shared by web and worker |

Compose overrides `TEMPORAL_ADDRESS` with `temporal:7233` inside the container network.

### Air development (optional)

Use Air when you want live reload on the host. Stop the Compose app processes first so you do not run two workers:

```sh
docker compose stop worker web
```

Then two terminals:

```sh
go run github.com/air-verse/air@v1.67.2 -c .air.worker.toml
```

```sh
go run github.com/air-verse/air@v1.67.2 -c .air.web.toml
```

Leave Temporal running:

```sh
docker compose up -d postgresql temporal temporal-ui
```

Air is **not** a module dependency. Build output stays under `./tmp/` (gitignored).

### Stop the stack

Preserve workflow history:

```sh
docker compose down
```

Reset the complete local environment for this project:

```sh
docker compose down -v
```
