# Reusable Activity Artifacts

## Use case

An opted-in Activity should reuse completed expensive work after an Activity retry or equivalent Workflow Run instead of repeating it.

## Model

Add RustFS with persistent local storage. `ReusableArtifactWorkflow` schedules five parallel `GenerateArtifactActivity` executions with stable IDs `artifact-000` through `artifact-004` and returns their compact artifact references.

Each Activity derives its RustFS key from:

```text
Namespace + Workflow ID + Activity ID + Activity Type + Activity Version
```

Workflow Run ID, Activity attempt, and Worker Build ID are excluded. Activity-version generation is outside this topic.

On an existing object, the Activity immediately returns its `ArtifactReference`. On a miss, it performs 20 seconds of heartbeating simulated work, optionally fails before publication, publishes a small artifact, optionally fails after publication, and returns the reference. One selected Activity receives the configured fault.

## Cases

| Case | What happens | Expected result |
|---|---|---|
| First execution | New experiment, version `v1`, no fault | All five Activities run for about 20 seconds and publish artifacts |
| Equivalent Workflow Run | Same Workflow ID and version `v1` start a new Run | All five Activities find their artifacts and complete quickly |
| Failure after publication | `artifact-002` publishes and fails on attempt 1 | Attempt 2 finds the artifact and completes quickly without heavy work |
| Failure before publication | `artifact-002` fails before publishing on attempt 1 | Attempt 2 finds nothing and repeats the 20-second work |
| Activity version changes | Same Workflow ID reruns with version `v2` | All five derive new keys, perform heavy work, and publish new artifacts |

## Acceptance criteria

- All cases run from the product UI against the real Temporal and RustFS stack.
- Temporal UI shows five Activity IDs, long cache misses, fast reuse, and the expected retry attempts.
- RustFS survives Worker restart and contains the referenced artifacts.
- Every successful Activity returns only the store and object key.
- No Temporal External Storage, reuse database, locking, retention policy, or Activity-version generator is added.
