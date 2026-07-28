# Workflow product vision and experiment roadmap

## Purpose

This document records the product and engineering vision agreed during the architecture discussion for this Temporal learning project. It is the baseline for future topic models and implementation plans.

The project is not intended to become one large Workflow that demonstrates every Temporal capability. Each experiment should have a focused use case, explicit behavior, and real-stack evidence.

Every topic follows the same sequence:

1. Research the Temporal and industry semantics.
2. Agree on a concrete model and expected behavior.
3. Implement the smallest focused vertical slice.
4. Verify it against the real Temporal stack and inspect the resulting History.

Implementation must not begin before the model is agreed. If an idiomatic design is unclear, the uncertainty must be surfaced in the plan rather than hidden behind speculative abstractions.

## Engineering guardrails

The existing architecture and boundaries must remain intact:

- Protobuf is the shared contract layer for Workflow requests, results, failures, status, and progress.
- Generated protobuf code is used by both `cmd/web` and `internal/workflows`.
- `cmd/web` is the Temporal client and product UI server. It does not run a Worker.
- `cmd/worker` is the only Worker process and owns Workflow and Activity registration.
- Workflow discovery remains registry-driven.
- HTTP routes preserve the separation between HTML UI responses and JSON API responses.
- Workflow-specific behavior belongs in Workflow code, not in the registry or UI.
- The registry describes how to start a Workflow. It does not contain static capability flags.
- Product-facing state must not expose raw Temporal infrastructure details unless they are useful.
- New abstractions must solve an observed requirement. They must not be added only because they might be useful later.
- Changes should be delivered as focused, atomic commits.

## Product direction

The broader product concept is an agent or user that can select and run one of several predefined Workflows.

The caller supplies the Workflow input, but does not need to understand the Workflow's internal DAG. The Workflow contract decides:

- Which inputs determine the logical operation identity.
- Which work is reusable.
- Which user commands are valid while the Workflow is running.
- Which progress and product states are exposed.
- Whether regeneration is meaningful for that Workflow.

A change to context-producing input, such as adding or removing source files, represents a new logical operation and may create a new Workflow identity. A change that only affects a declared nondeterministic generation step, such as changing an LLM writing instruction, may be handled inside the same long-lived Workflow when that Workflow explicitly models regeneration.

This agent-facing behavior is context for the architecture. It is not the next implementation target.

## Focused Workflow experiments

Features should not be added indiscriminately to every existing Workflow.

The current simple examples should remain understandable:

- Greeting demonstrates one Activity.
- Simple diamond demonstrates static dependency ordering and parallel branches.
- Conditional branch demonstrates runtime branching.
- Dynamic fan-out demonstrates runtime branch creation and fan-in.
- Fan-out policy demonstrates terminal failure aggregation behavior.

New behavior should be introduced through focused Workflow examples when it changes the Workflow's fundamental control flow. Regeneration, long-lived interaction, and storage-backed aggregation are examples of behavior that deserve dedicated Workflow models rather than being forced into every existing example.

## Live product status

### Workflow-owned status

A running Workflow owns its product-facing status. The shared protobuf status contract can contain:

- Product state.
- Current phase.
- Current friendly step.
- User-facing message.
- Progress counters.
- Percentage.
- Monotonic revision.
- Actions currently valid in that state.

The status Query is read-only. It does not mutate Workflow state or add a Query event to History.

The UI must not infer controls from static registry metadata. It asks the running Workflow for its current state and valid actions.

For example:

```text
RUNNING
    actions: Pause, Cancel

PAUSED
    actions: Resume, Cancel

READY
    actions: Regenerate, Cancel

REGENERATING
    actions: Cancel
```

The returned actions are presentation hints. Workflow Update validators or native Temporal APIs remain authoritative. A stale UI action must be rejected safely if the state changed before the user clicked it.

### SSE instead of browser polling

The browser does not poll Workflow Queries on a timer.

The implemented live-status flow is:

```text
Temporal History changes
    -> server-side History watcher wakes
    -> cmd/web Queries the current product status
    -> status revision is compared
    -> a complete status snapshot is published over SSE
    -> the browser reconciles the correct run card
```

Raw Temporal History events are not sent to the browser. They are only wake-up signals. The browser receives product-facing snapshots.

The live transport uses Server-Sent Events because status is server-to-browser communication. Browser-to-server commands continue to use ordinary HTTP requests.

The SSE infrastructure uses:

- One long-polled Temporal History watcher per Workflow ID and Run ID.
- Shared watchers across connected subscribers.
- One multiplexed browser SSE connection for several active runs.
- Reconciliation by Workflow ID, Run ID, and status revision.
- Coalescing so slow clients receive the newest state without unbounded queues.
- Session storage for active run descriptors and reconnect behavior.
- Terminal success and structured failure events.

The SSE and asynchronous-start foundation is implemented and verified against the real Compose stack.

## Temporal control terminology

The following terms must remain distinct.

### Query

A synchronous, read-only request for current Workflow state.

Use it for:

- Status.
- Progress.
- Current phase.
- Current product result metadata.
- Currently valid actions.

### Signal

A durable asynchronous message to an open Workflow.

Use it when the sender only needs the message accepted by Temporal and does not require Workflow-level validation or a returned result.

Signals remain an important experiment, but they are not automatically the best mechanism for product buttons that require acknowledgement.

### Update

A durable request/response interaction with an open Workflow.

An Update can:

- Validate the requested transition.
- Mutate Workflow state.
- Return an acknowledgement or rejection.

Updates are a good fit for cooperative product commands such as regeneration and potentially application-level pause/resume.

### Cancellation

Cancellation is a native Temporal operation, not a Query, Signal, or Update.

Cancellation is cooperative:

- The Workflow receives cancellation through its context.
- Activities receive cancellation according to Activity behavior and heartbeat/cancellation handling.
- Cancellation does not automatically roll back completed external side effects.
- Cleanup or compensation must be modeled explicitly where required.

### Termination

Termination immediately stops the Workflow without allowing Workflow cleanup code to run. External lifecycle cleanup therefore needs retention policies or an out-of-band janitor when termination is part of the experiment.

### Activity retry

An Activity Execution may have several attempts under its Retry Policy. The Workflow sees one Future and eventually receives one terminal outcome.

Activity retries are the primary retry mechanism for failure-prone external work.

### Workflow retry

Workflow Executions do not retry by default, and the agreed default for this project is one Workflow attempt unless a dedicated experiment says otherwise.

Automatically restarting a complete 1,000-Activity operation is not the default recovery policy.

### Product retry

A future product retry may start a failed or canceled logical operation again while reusing valid persisted results. This behavior depends on the result-storage model and is not yet implemented.

### Replay

Replay reconstructs Workflow state from Event History and verifies deterministic compatibility. It is a developer and deployment mechanism, not a product button.

### Reset

Temporal Reset is an operator-level History operation:

- Select a Workflow Task event.
- Copy History up to the reset point.
- Create a new Run under the same Workflow ID.
- Replay the retained History using currently deployed Workflow code.
- Continue from the reset point.

Reset is not the same as retry or regeneration. It remains a separate operator experiment.

## Pause and resume research

The current Temporal API dependency contains experimental native Workflow pause and unpause RPCs.

The documented native behavior includes:

- Workflow status changes to paused.
- A pause event is added to History.
- New Workflow and Activity Tasks are not dispatched.
- A currently executing Workflow Task may complete.
- Activity pause behavior is applied to running or retrying Activities.
- Server-side events continue to be processed.
- Queries and Updates are rejected while the Workflow is natively paused.

There are also Activity-level pause, unpause, and reset APIs.

This changes the earlier assumption that pause must always be implemented cooperatively inside a specific Workflow. Native Workflow pause could potentially apply to any open Workflow Execution.

Open questions remain:

- Whether the project's Temporal Server `1.29.2` implements these newer experimental RPCs.
- How the high-level Go client should access them, because it does not currently expose convenience methods.
- How non-heartbeating and heartbeating Activities behave in the real stack.
- How SSE reports paused status when Queries are rejected.
- Whether native pause or cooperative pause better matches a specific product use case.

No pause/resume implementation should begin until this is researched and modeled. Pause/resume is not part of the next fan-out-policy implementation slice.

## Fan-out failure-policy experiment

This is the next implementation focus.

### Workload

The experiment runs 1,000 `FaultInjectionActivity` executions.

Each Activity has:

- A unique Activity ID.
- A shared experiment seed.
- An Activity attempt number.
- A deterministic probability selection derived from seed, Activity ID, and attempt.
- Its own Activity Retry Policy and timeout behavior.

The existing fault harness supports:

- Success.
- Retryable application failure.
- Non-retryable application failure.
- Panic.
- Start-to-Close timeout.
- Heartbeat timeout.
- Wait for cancellation.

### Campaigns

The experiment needs at least two campaign types.

#### All-success campaign

Every branch succeeds. This demonstrates the desired normal result:

```text
planned = 1000
succeeded = 1000
failed = 0
canceled = 0
```

All three policies should produce a successful aggregate and completed Workflow when every branch succeeds.

#### Mixed-fault campaign

A seeded probability profile produces a reproducible mix of:

- Immediate successes.
- Retryable failures that later succeed.
- Retry exhaustion.
- Non-retryable failures.
- Panics.
- Start-to-Close timeouts.
- Heartbeat timeouts.
- Fail-fast sibling cancellation.

The generator must make important cases observable. It must not use a probability configuration in which an all-success run is effectively impossible when the all-success path is the behavior being demonstrated.

The same seed, Activity IDs, retry configuration, timeout configuration, and workload should be usable across policy comparisons.

### Three policies

#### Fail-fast

- Activities are scheduled according to the agreed fan-out model.
- The first terminal failure observed by the Workflow Selector triggers the policy.
- When several Futures are already ready in one Workflow Task, the Selector's deterministic choice is not claimed to be exact History completion order.
- Unfinished siblings are canceled.
- Already completed results remain completed.
- The triggering Activity identity and normalized failure are preserved.
- Normal full-data aggregation does not run when required inputs are missing, unless a later model explicitly defines partial aggregation for that use case.
- The Workflow fails.

#### All-settled

- Every Activity receives its complete retry chain.
- Every terminal Future is collected.
- All successful branch outputs are consumed by the aggregator.
- Failure metadata is included in the aggregate context.
- The Workflow completes with a complete or partial aggregate, depending on branch outcomes.

#### All-settled-then-fail

- Every Activity receives its complete retry chain.
- Every terminal Future is collected.
- The aggregator consumes all available successful outputs.
- A useful aggregate artifact may still be produced.
- The Workflow fails afterward when required branches failed.
- The structured failure preserves a compact aggregate reference and summary.

### Error propagation

A terminal Activity failure must preserve useful information:

- Activity ID.
- Input index.
- Failure kind.
- Application failure type.
- Timeout type.
- Non-retryable flag.
- Safe message.
- Final attempt where available.

Fail-fast must identify the triggering branch. All-settled results must classify every terminal branch without exposing raw Go `error` values in protobuf contracts.

### Real-stack evidence

For each run, capture:

- Workflow ID and Run ID.
- Policy and campaign.
- Final Workflow status.
- Planned, succeeded, failed, and canceled counts.
- Activity attempts visible through Temporal and Worker logs.
- Timeout and cancellation types.
- Aggregation behavior.
- History event count.
- History size.
- Payload sizes.
- Runtime.
- Temporal UI link.

The current implementation remains incomplete because probability input is not wired through the Workflow request, the policy Workflow currently limits Activities to one attempt, and its result still carries all detailed outcomes directly.

## Real aggregation and result storage

A fan-in aggregator must consume actual branch outputs. It cannot be reduced to counters and representative samples.

Counters may be the final user-facing result, but they do not replace the data that aggregation needs.

### Temporal as control plane

Temporal History may carry:

- Small values.
- Compact metadata.
- Database record references.
- RustFS artifact locations.
- Checksums.
- Sizes.
- Product status and progress.

Large branch outputs should not be combined into one Temporal Activity input or Workflow result simply because the first 1,000 examples fit under current payload limits.

### SQLite

SQLite will simulate application-owned durable side effects in the local lab.

Use it for data that remains useful after Workflow completion:

- Application records.
- Imported or processed entities.
- Durable business output.

Activities use natural business checks where appropriate. If the expected result already exists, an Activity retry may return it instead of repeating the side effect.

SQLite is a local adapter. A production multi-pod system would use a shared database such as PostgreSQL rather than a pod-local SQLite file.

### RustFS

RustFS will provide S3-compatible durable artifact storage for the local lab.

Use it for large intermediate data whose primary consumer is downstream aggregation:

- Converted documents.
- LLM context bundles.
- Large JSON.
- Analysis partitions.
- Temporary generated files.

These artifacts may be temporary in lifecycle but must remain durable while a Workflow may pause, retry, lose a Worker, or delay aggregation for days.

### Local scratch

Local filesystem scratch is valid only inside one Activity attempt:

- Downloads.
- Decompression.
- Temporary conversion files.
- Reconstructable caches.

Anything needed by another Activity or a later retry must not exist only in local scratch.

### Activity result contract

At the start of an Activity, the Activity owner decides whether existing work can be reused.

The simple behavior is:

```text
Check whether the logical output already exists
    -> if it exists, return its small value or location
    -> otherwise perform the work
    -> persist when necessary
    -> return a value or reference
```

The Activity owner chooses the correct behavior:

- Small pure output may be returned through Temporal.
- Durable business output may be represented by a SQLite record reference.
- Large transient output may be represented by a RustFS location.
- Cheap or harmless work may simply execute again.

The project does not introduce a generic distributed lock or an artificial operation-key table for every database operation.

### Aggregation Activity

The aggregation Activity receives compact values and references, then reads the actual branch outputs:

- Inline values from its input.
- Durable records from SQLite.
- Large artifacts from RustFS.

It validates and consumes the complete available result set, performs the real reduction, and returns a compact summary or stored aggregate reference.

Potential aggregations include:

- LLM report generation.
- Data analysis.
- Document merging.
- Statistics.
- Deterministic checksum and count verification.

### Cleanup

Initial lifecycle behavior:

- Successful aggregate: delete transient RustFS branch artifacts after the aggregate is safely committed.
- SQLite business records remain.
- Failed aggregate: retain RustFS artifacts for retry.
- Canceled Workflow: retain artifacts initially.
- Local scratch is always disposable.
- Terminated Workflow cleanup eventually requires storage retention or a janitor because Workflow cleanup code cannot run.

## Payload and History limits

The project must measure rather than assume scale safety.

Important Temporal limits include:

- Individual payload limits.
- Transaction and transport limits.
- Workflow History event count.
- Workflow History size.
- Pending Activity limits.
- Replay cost and Worker memory.

A thousand compact Activity references may fit today, but unbounded repeated `ActivityOutcome` messages in one result are not a scalable aggregate contract.

The aggregator must consume the actual data while keeping Temporal payloads bounded through values, references, and eventually a manifest when measurement shows it is required.

Continue-As-New is a later tool for long-lived workflows and repeated regeneration, not a replacement for external large-result storage.

## Regeneratable Workflow behavior

Regeneration is not valid for every Workflow and should not be added as a generic flag to every Activity.

The agreed Temporal-native model is normal Workflow control flow:

```text
run reusable preparation once

loop:
    run the nondeterministic Activity with current instructions
    run every downstream dependent step
    publish the new result
    wait durably for another regeneration command
```

### Diamond example

For a regeneratable diamond:

```text
prepare        deterministic and executed once
branch A       deterministic and executed once
branch B       nondeterministic and executed per regeneration
finalize       depends on branch B and executes per regeneration
```

Initial execution:

```text
prepare
branch A
branch B with instruction A
finalize
wait
```

Regeneration:

```text
reuse prepare state from History
reuse branch A state from History
schedule a new branch B Activity with instruction B
schedule a new finalize Activity
publish the new result
wait
```

Temporal does not cache Activities based on function name and input. A completed Activity command is reused during replay because it is in History. A new `ExecuteActivity` command schedules a new Activity Execution.

Therefore:

- No prompt hash is required in Activity ID to force execution.
- No generic DAG invalidation engine is required.
- No product database generation number is required.
- No generic `forceRerun` flag belongs on every Activity.

The Workflow remains open while it supports regeneration and receives the new instruction through an Update. Product state may be `READY` while Temporal state remains open and durably waiting.

If context-producing input changes, such as adding source files, the caller starts a new logical Workflow. If only the declared nondeterministic generation instruction changes, the existing Workflow may reuse its prepared context and run the generation suffix again.

## Static registry capability flags are rejected

The registry must not contain booleans such as:

```text
CanPause
CanResume
CanCancel
CanRetry
CanRegenerate
HasDurableOutputs
HasTemporaryArtifacts
```

Reasons:

- Action availability changes with runtime state.
- Static flags duplicate Workflow behavior.
- Registry metadata can drift from actual handlers and validators.
- Storage choices are implementation details, not UI capabilities.

The running Workflow's status Query returns actions valid in its current state. Closed-run actions come from terminal status and structured failure semantics.

## Explicitly rejected designs

Do not reintroduce these without a new agreed model:

- One giant Workflow containing every experiment.
- Static control capability flags in the registry.
- Browser polling for live status.
- Raw Temporal History events sent to the product UI.
- A generic `forceRerun` flag on every Activity.
- Product database generation numbers for regeneration.
- A generic distributed lock around all Activities.
- A custom DAG invalidation engine for LLM regeneration.
- A generic operation-key table for every database write.
- Child Workflows used only to run the three fan-out policies.
- Returning all large branch outputs in one Temporal payload.
- Replacing actual aggregation input with only counters or samples.
- Treating Retry, Replay, Reset, and Regenerate as synonyms.
- Implementing before the topic model and acceptance criteria are agreed.

## Roadmap and current status

### Completed foundation

- Protobuf operation status and progress contracts.
- Workflow status Query for current examples.
- Asynchronous Workflow starts.
- Temporal History long-poll watchers.
- Multiplexed SSE endpoint.
- Live browser run reconciliation.
- Session reconnect state.
- Real-stack success, multiplexing, progress, and structured-failure verification.
- Atomic commits for status contracts, SSE backend, and UI reconciliation.

### Next implementation focus

Complete the 1,000-Activity fan-out failure-policy experiment only:

1. Agree on compact request and generator model.
2. Wire seeded probabilities into `FaultInjectionActivity` inputs.
3. Add all-success and mixed-fault campaigns.
4. Configure real Activity retries and timeout windows.
5. Run the same workload under all three policies.
6. Preserve correct structured terminal failures.
7. Introduce real result aggregation without unsafe payload growth.
8. Expose the experiment through the existing asynchronous UI and SSE status flow.
9. Verify every case against the real Compose stack.
10. Commit the vertical slice atomically.

### Later focused topics

After the fan-out policy experiment:

- SQLite, RustFS, and real aggregation.
- Native Workflow/Activity pause research against the actual server.
- Cooperative pause only if a product use case requires different semantics.
- Pause/resume/cancel UI controls.
- Product retry using persisted outputs.
- Dedicated regeneratable Workflow.
- Reset and replay experiments.
- Worker crash and storage restart recovery.
- Workflow code versioning and patching.
- Continue-As-New for long-lived operations.
- Agent integration and structural Workflow identity.

Each later item requires its own agreed topic model before implementation.
