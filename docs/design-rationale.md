# Design Rationale

> **Status:** Durable architecture rules and the failure modes they answer

This document explains why the platform is shaped the way it is. The other documents describe what exists; this one records what was rejected and why, so the rules are not re-litigated by accident.

## The failure mode this project answers

A team adopts a durable execution engine, then keeps its own tables "just for the UI". A run row here, a node status there, a variables table to pass values between steps, a ledger for deduplication, a queue for scheduling, a feed for progress. Each addition is individually reasonable.

The end state is a database-backed workflow interpreter wearing a Temporal costume. Durability, retries, parent and child lifecycle, cancellation, deduplication, and result propagation all exist twice. Each copy is hand-built, each is subtly wrong in a different way, and the symptoms surface as unrelated bugs across every layer.

The observed consequences are consistent:

- Workflows that always complete successfully, because branch failures are caught and written as rows. The engine's UI, metrics, retries, and failure-aware ID policies go blind.
- Results delivered by best-effort signals instead of futures, so a lost message hangs a step until its execution timeout.
- Deduplication rebuilt several times, with every generation still deployed and disagreeing.
- Force flags, rerun flags, and regeneration flags accumulating on every unit of work because no layer owns identity.
- Cancellation that updates a row and stops nothing.
- Progress represented four ways, none authoritative.
- Documentation describing the intended system rather than the running one.

## The correction

One ownership rule generates every other rule in this repository:

> Temporal owns execution state. The Workflow owns product state. Everything outside them holds definitions, durable business records, and large artifacts—nothing that duplicates execution.

## Failure mode to experiment

Each Workflow here isolates one execution concern and runs against a real stack. The final column states what the repository currently contains, not a benchmark result.

| Failure mode | Answer in this repository | State |
|---|---|---|
| Failures recorded as rows; execution reports success | Structured protobuf failures raised as Application Errors (`internal/workflows/failure.go`), classified Activity error types, terminal failure decoding in web | Demonstrated |
| Results routed by best-effort signals | Branch results awaited as futures (`simple_diamond.go`, `dynamic_fan_out.go`) | Demonstrated |
| Retry behavior differing by dispatcher; unset attempt limits meaning unlimited | Explicit per-Activity retry and timeout configuration across a 1,000-branch fault-injection campaign (`fan_out_policy.go`, `fan_out_campaign.go`) | Demonstrated |
| Terminal failure semantics undefined for fan-out | Three named policies with stated aggregation and cancellation behavior | Demonstrated |
| Deduplication layers competing | Business-key identity: artifact keys exclude Run ID, attempt, and Build ID; a durable report reuses a matching row and rejects a conflicting one (`reusable_artifact.go`, `durable_report.go`) | Demonstrated |
| Execution state duplicated in application tables | No application run database. Temporal holds execution state; SQLite holds business records only | Structural |
| Progress represented several ways | One Workflow-owned status Query with a monotonic revision, published as complete SSE snapshots (`status.go`, `cmd/web/monitor.go`) | Demonstrated |
| Static capability flags drifting from runtime behavior | The running Workflow returns the actions valid in its current state; the registry carries start contracts only | Structural |
| Large payloads travelling through execution history | References to object storage and durable records; bounded aggregates and sampled failures | Demonstrated |
| Documentation describing intent as behavior | Every document carries a status line and states what is not implemented | Structural |

## Rejected designs

Do not reintroduce these without a stated model and a reason against the entry.

| Rejected | Reason |
|---|---|
| One Workflow demonstrating every capability | Each concern needs an isolated, readable example |
| Static capability flags in the registry (`CanPause`, `CanRetry`, `CanRegenerate`) | Action availability is runtime state; static metadata drifts from the validators that actually decide |
| Browser polling for live status | Status is server-to-browser; History changes already provide the wake-up |
| Raw History events sent to the product UI | History is an operator record, not a frontend event contract |
| A generic `forceRerun` flag on every Activity | Reuse is per-Activity business behavior, decided at Activity entry |
| Generation numbers in an application database to drive regeneration | Regeneration is ordinary Workflow control flow; a new Activity call already schedules new work |
| A custom DAG invalidation engine | The same; the engine has no observed requirement |
| A generic distributed lock around every Activity | Idempotency belongs to the destination, not to a global gate |
| A generic operation-key table for every write | Natural business keys already identify the output |
| Child Workflows used only to run fan-out policies | Policies are control flow, not a lifecycle boundary |
| Returning all branch outputs in one payload | Bounded payloads are a hard constraint, not a limit to discover later |
| Aggregation reduced to counters or samples | Fan-in must consume actual outputs; counters may be the result, not the input |
| Treating retry, replay, reset, and regenerate as synonyms | Four distinct mechanisms; see the control terms in `execution-semantics.md` |
| Positional detail lists on Activity failures | The SDK leaves surplus decode targets untouched and still reports success, so a drifted shape is published as a zero value that no check can catch |
| Deadlines that include queue time on a large fan-out | Schedule-to-close makes a branch's outcome a function of Worker load rather than of the branch |
| Reporting cancellation because a deadline passed | It races Temporal's timeout, so the same fault classifies as a timeout or a cancellation across runs, and winning the race ends the retry chain early |
| One status Query per Workflow Task | Queries scale with Activity count, not with status changes, and are dispatched to the Worker running those Activities |
| Abstractions added before an observed requirement | Speculative structure is the cost being avoided |

## Not yet answered

These failure modes are real and this repository does not currently disprove them. Each needs its own model before implementation.

| Open question | Why it matters |
|---|---|
| Cancellation, pause, and resume through a run tree | Cancellation that stops nothing is the defect; this repository has no parent and child lifecycle to test it against |
| Workflow versioning, patching, and replay tests | A control-flow edit can break replay for in-flight runs. Long-lived operations make this the highest-severity untested area |
| Run attach: joining work already in flight | Work identity expressed as the Workflow ID is what removes competing deduplication layers |
| Read models without re-inverting | The engine eventually needs queryable run history. Building it wrong is exactly how the original inversion started |
| Search attributes | Without them there is no way to query runs by business dimension |
| Continue-As-New | The bound on History growth for long-lived and repeatedly regenerated operations |
| Admission control and concurrency pools | Scheduling belongs in the engine, not in a satellite queue |
| Compiled definitions versus runtime composition | Workflows here are compiled Go. An agent composing a novel graph at runtime is a different execution model, and the seam between them is untested |

## Code map

- Structured failures: `internal/workflows/failure.go`
- Product status: `internal/workflows/status.go`
- Fan-out policies: `internal/workflows/fan_out_policy.go`, `internal/workflows/fan_out_campaign.go`
- Reuse and durable records: `internal/workflows/reusable_artifact.go`, `internal/workflows/durable_report.go`
- Start contracts: `internal/workflowcatalog/catalog.go`
- Live status transport: `cmd/web/monitor.go`, `cmd/web/events.go`
