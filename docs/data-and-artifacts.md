# Data and Artifacts

> **Status:** Architecture rules; storage-backed artifacts are not implemented

Temporal is the execution control plane. It should carry commands, small values, status, and durable references—not become bulk object storage.

## Put data in the right place

| Data | Location |
|---|---|
| Small control values and compact results | Temporal payloads |
| Durable business records | Application database |
| Large intermediate or final artifacts | Object storage |
| Attempt-local downloads and conversion files | Worker-local scratch |

Anything needed by another Activity or a later retry must not exist only on local scratch.

The repository does not currently include an application database, RustFS, or another object store. Those are future adapters, not supported features today.

## Reusable Activity results

Reuse is opt-in business behavior. At Activity entry:

1. Derive the logical output identity.
2. Validate an existing artifact if present.
3. Otherwise perform the work and publish atomically.
4. Return a compact record or artifact reference.

A reusable identity should reflect tenant scope, immutable business inputs, output schema version, and implementation version. Workflow ID or Activity ID belongs in the key only when it is part of the business reuse boundary. Run ID, attempt number, and Worker Build ID usually should not invalidate retry reuse.

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
