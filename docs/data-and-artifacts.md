# Data and Artifacts

> **Status:** Architecture rules; RustFS artifact reuse and one SQLite-backed durable report pipeline are implemented

Temporal is the execution control plane. It should carry commands, small values, status, and durable references—not become bulk object storage.

## Put data in the right place

| Data | Location |
|---|---|
| Small control values and compact results | Temporal payloads |
| Durable business records | Application database |
| Large intermediate or final artifacts | Object storage |
| Attempt-local downloads and conversion files | Worker-local scratch |

Anything needed by another Activity or a later retry must not exist only on local scratch.

The local stack includes RustFS for reusable artifacts and a persistent SQLite database for the `DurableReportWorkflow` experiment. General-purpose storage adapters are not implemented.

## Reusable Activity results

Reuse is opt-in business behavior. At Activity entry:

1. Derive the logical output identity.
2. Validate an existing artifact if present.
3. Otherwise perform the work and publish atomically.
4. Return a compact record or artifact reference.

A reusable identity should reflect tenant scope, immutable business inputs, output schema version, and implementation version. Workflow ID or Activity ID belongs in the key only when it is part of the business reuse boundary. Run ID, attempt number, and Worker Build ID usually should not invalidate retry reuse.

## Current reusable-artifact experiment

`ReusableArtifactWorkflow` runs five Activities with stable IDs. Each Activity looks up a RustFS object derived from Temporal namespace, Workflow ID, Activity ID, Activity type, and Activity version. Workflow Run ID, Activity attempt, and Worker Build ID are excluded.

An existing object is reused immediately. A miss performs the configured heartbeating work and publishes the object before returning its compact reference. This experiment intentionally has no locking, retention policy, or automatic Activity-version generation.

## Current durable-report pipeline

`DurableReportWorkflow` generates five reusable RustFS artifacts, passes their compact references to an Activity that consumes all five objects in deterministic order, and persists the resulting semantic digest under an immutable business `report_id` in SQLite. A matching row is reused; the same `report_id` with different semantic content fails as a business conflict.

The SQLite file uses a persistent Docker volume and survives Worker replacement. It demonstrates business-key idempotency for the local single-Worker stack; production multi-Worker deployment requires a shared application database.

## Failure windows

| Failure | Retry behavior |
|---|---|
| Before publication | No valid output exists; perform the work again |
| After publication but before Activity completion is recorded | Validate and reuse the published output |
| After implementation or schema version changes | Derive a new identity and create a new output |
| Partial or corrupt publication | Reject it and rebuild safely |

Production publication also needs checksums, atomic writes, authorization, tenant isolation, and an explicit retention policy.

## Bounded aggregation

Fan-in must consume actual successful outputs. Counters alone are not aggregation input.

- Combine inputs in deterministic order.
- Exclude wall-clock metadata from semantic digests.
- Pass references when branch data is large.
- Return a compact summary or aggregate reference.
- Bound failure samples and repeated fields.

A compact Workflow result does not make History small. Individual Activity events and payloads still contribute to History size, replay cost, and Worker memory.

## Cleanup

Delete transient branch artifacts only after the final aggregate is durably committed. Retain inputs needed for retry after failure or cancellation. Terminated Workflows cannot run cleanup code, so production storage needs retention rules or an out-of-band janitor.
