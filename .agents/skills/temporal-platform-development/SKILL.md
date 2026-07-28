---
name: temporal-platform-development
description: Design, implement, and verify focused changes to this Temporal orchestration platform without creating planning-document sprawl.
---

# Temporal Platform Development

Work from product contracts, then prove behavior in code and tests.

## Start with the boundary

Read only the relevant canonical documents:

- `docs/architecture.md`
- `docs/adding-a-workflow.md`
- `docs/execution-semantics.md`
- `docs/data-and-artifacts.md`
- `docs/http-api.md`

State whether the request changes current behavior, a public contract, or an unimplemented design direction. Do not present planned capabilities as supported.

## Model only when needed

For ambiguous behavior, agree on the use case, guarantees, failure cases, and observable result before coding. For a clear implementation request, proceed without an artificial approval gate.

Keep design discussion in chat. Do not create research notes, topic files, implementation plans, roadmaps, or verification reports.

## Temporal correctness

Before implementation, account for:

- Workflow determinism and replay compatibility.
- Activity timeouts, retries, heartbeats, and cancellation.
- At-least-once Activity execution and side-effect idempotency.
- Workflow and Activity failure boundaries.
- Compensation after partial success when the business requires it.
- Payload size, History growth, and large-data references.
- Contract compatibility across web and Worker deployments.

External I/O belongs in Activities. Product state belongs to the Workflow. Registry metadata must not duplicate runtime behavior.

## Implement and verify

Make the smallest coherent vertical change. Use the strongest applicable validation:

1. Activity unit tests.
2. Temporal Workflow test-environment tests.
3. Replay tests for deployed Workflow changes.
4. HTTP and SSE tests for public transport changes.
5. Real-stack verification when behavior depends on Temporal Server semantics.

Fix implementation bugs autonomously. Stop for discussion only when evidence requires changing the agreed behavior or scope.

## Documentation hygiene

Update one canonical document only when a durable user-facing contract or architecture rule changes. Prefer code, tests, issues, and pull requests for details that will expire.

Report changed files, validation commands, and relevant limitations in chat. Do not create a document just to record that work happened.
