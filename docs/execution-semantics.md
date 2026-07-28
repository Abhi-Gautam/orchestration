# Execution Semantics

> **Status:** Current patterns and platform rules

Workflow code defines control flow. Temporal records its decisions so the same code can reconstruct state during replay.

## Dependency patterns

| Pattern | Rule |
|---|---|
| Sequence | Wait for a required result before scheduling its dependent work |
| Parallel branches | Schedule independent Futures before waiting for either |
| Fan-in | Start downstream work only after its required Futures settle |
| Conditional branch | Choose a path from recorded Workflow state or an Activity result |
| Dynamic fan-out | Derive branch count at runtime, schedule branches, then collect them |

Scheduling 1,000 Activities does not guarantee 1,000-way execution. Worker capacity, task-queue pressure, and server limits determine physical concurrency. Large fan-outs also increase History size and replay cost.

Use a Temporal timer for a durable delay. Use a waiting Activity only when the work must occupy an Activity execution or model an external operation.

## Activity outcomes

One Activity Execution may contain several attempts. Its Workflow Future exposes one terminal outcome: a success, exhausted retry chain, non-retryable failure, timeout, or cancellation.

Fan-out aggregation acts on those terminal Futures, not on individual retry attempts.

## Fan-out failure policies

| Policy | Behavior |
|---|---|
| Fail-fast | Preserve the first terminal failure selected by Workflow code, cancel unfinished siblings, and fail without claiming a complete aggregate |
| All-settled | Observe every terminal Future, aggregate available successful outputs, and complete with failure counts |
| All-settled-then-fail | Build the same aggregate, then fail when the Workflow contract requires every branch |

Completed work is not rolled back when another branch fails. Compensation is a separate business policy.

When several Futures are ready in one Workflow Task, deterministic Selector behavior must not be described as exact wall-clock completion order.

## Failure contract

A useful terminal failure preserves:

- Branch identity and input index.
- Application or timeout type.
- Retryability.
- A safe message.
- Final attempt when available.

Large fan-outs should return bounded category counts and representative samples, not every detailed failure.

## Control terms

| Term | Meaning |
|---|---|
| Query | Read current Workflow state without mutating it |
| Signal | Deliver a durable asynchronous message |
| Update | Validate, mutate, and return an acknowledged result |
| Cancellation | Cooperatively ask Workflow and Activity code to stop |
| Termination | Stop immediately without Workflow cleanup |
| Activity retry | Retry one external operation under its Activity policy |
| Workflow retry | Start another Workflow attempt under a Workflow retry policy |
| Replay | Reconstruct state from History and check deterministic compatibility |
| Reset | Create a new Run from an earlier Workflow Task boundary |

The current HTTP API exposes start and monitoring only. Signal, Update, cancellation, termination, retry, and reset product endpoints are not implemented.

## Code map

- Static fan-out: `internal/workflows/simple_diamond.go`
- Runtime branching: `internal/workflows/conditional_branch.go`
- Dynamic fan-out: `internal/workflows/dynamic_fan_out.go`
- Failure policies: `internal/workflows/fan_out_policy.go`
