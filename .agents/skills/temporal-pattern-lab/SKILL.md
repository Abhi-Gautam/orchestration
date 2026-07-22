---
name: temporal-pattern-lab
description: Research, model, implement, verify, and review one Temporal workflow/DAG topic at a time. Use for any topic listed in the repository README or when the user wants to deeply learn a Temporal pattern, guarantee, failure mode, testing strategy, operational behavior, or product-integration concern through a phase-gated example.
---

# Temporal Pattern Lab

Use this skill to study exactly one Temporal topic through four gated phases:

1. Research
2. Example modeling
3. Implementation and verification
4. Grill and review

Do not skip phases or silently advance between them. At the end of each phase, summarize the artifact, list unresolved questions, and wait for the user's explicit approval before beginning the next phase.

## Ground rules

- Work on one clearly named topic at a time.
- Read the repository `README.md` when the topic comes from the project topic list.
- Create a lowercase hyphenated topic slug and store artifacts under `docs/topics/<topic-slug>/`.
- Use `topic-template.md` in this skill directory as the artifact outline.
- Treat the user's desired behavior as a requirement to investigate, not proof that Temporal supports it directly.
- Clearly distinguish facts, inferences, recommendations, experiments, and unresolved questions.
- Prefer a small example that exposes the important semantics over a large realistic application.
- Explain Temporal guarantees separately from guarantees our application code must provide.
- Include failure, retry, cancellation, replay, idempotency, concurrency, history-growth, and observability implications whenever they apply.
- Do not alter the shared Compose stack merely for convenience. Explain and obtain approval for infrastructure changes that materially change the learning environment.
- Never claim a behavior was verified unless it was run and observed.

## Phase 0: Select and frame the topic

Before research, restate the selected topic in one sentence and establish:

- The concrete question we are answering.
- Why the behavior matters.
- What is in scope.
- What is explicitly out of scope for this pass.
- Terms that appear ambiguous.
- Any user assumptions that need validation.

Ask only essential framing questions. If the topic is sufficiently clear, proceed with stated assumptions rather than blocking.

## Phase 1: Research

Create `docs/topics/<topic-slug>/01-research.md`.

Research and explain:

1. **Problem definition**
   - What the topic means outside Temporal.
   - What “solving it” means in observable terms.
   - Actors, resources, failure boundaries, and desired guarantees.
2. **Industry patterns**
   - Common implementation patterns.
   - When each pattern is appropriate.
   - Known failure modes and operational tradeoffs.
3. **Temporal model**
   - Relevant Temporal primitives and exact semantics.
   - What Temporal provides automatically.
   - What must be implemented in Workflow code, Activity code, worker configuration, infrastructure, or product code.
   - Important non-features and misconceptions.
4. **Alternatives and tradeoffs**
   - Viable Temporal approaches.
   - Pros, cons, complexity, scaling limits, history implications, and operational impact.
5. **Recommendation for this lab**
   - Recommended pattern to model first.
   - Why it is the best teaching example.
   - Alternatives intentionally deferred.
6. **Open questions**
   - Decisions that need user input.
   - Claims that require an experiment.
7. **Sources**
   - Link every major Temporal-specific claim to a source.
   - Record relevant product/SDK versions and the access date.

### Research source order

Prefer sources in this order:

1. Official Temporal documentation.
2. Go SDK API documentation and source.
3. Official Temporal samples.
4. Temporal server/SDK source and release notes.
5. Temporal community forum for nuanced behavior, labeled as community guidance.
6. General distributed-systems literature for non-Temporal patterns.

Use current sources that are compatible with this project's pinned server and Go SDK. Call out version-sensitive behavior.

### Phase 1 gate

Do not write implementation code. Present:

- A concise conclusion.
- Recommended approach.
- Important alternatives.
- Open questions.
- Path to `01-research.md`.

Then wait for questions and explicit approval to model the example.

## Phase 2: Model the example

Create `docs/topics/<topic-slug>/02-model.md`.

Model an example detailed enough that implementation becomes mechanical. Include:

- Learning objective and non-goals.
- Actors and components.
- Inputs and outputs.
- Workflow, Activity, Child Workflow, API, database, and worker boundaries.
- A Mermaid flowchart or sequence diagram.
- Task Queue and concurrency assumptions.
- Workflow IDs, Run IDs, idempotency keys, and business IDs where relevant.
- Happy-path timeline.
- Failure matrix covering retries, non-retryable failures, timeouts, cancellation, worker death, and duplicates as applicable.
- Error-propagation policy.
- State/progress model.
- User-visible behavior versus Temporal UI/operator-visible behavior.
- Expected Temporal event-history landmarks.
- Tests and manual experiments.
- Acceptance criteria with observable outcomes.
- Alternatives excluded from the first example.
- Questions requiring agreement.

Do not implement production code during modeling. Small pseudocode snippets are allowed only when they remove ambiguity.

### Phase 2 gate

Walk the user through the model. Revise it until the user explicitly approves it. Do not begin implementation merely because the model seems complete.

## Phase 3: Implement and verify

Implement only the approved model.

1. Inspect the existing project and preserve established structure.
2. State the focused file-change plan.
3. Add or modify the minimum code needed for the example.
4. Keep deterministic orchestration in Workflows and side effects in Activities.
5. Add tests described in the approved model.
6. Format, compile, and run focused tests.
7. Start or reuse the project's Compose stack.
8. Run the worker and example client/API.
9. Exercise the approved happy path and failure scenarios.
10. Verify results through the Temporal frontend/CLI, not only application output.
11. Capture Workflow ID, Run ID, result/status, and a direct Temporal UI link when available.
12. Record commands, observed behavior, and limitations in `docs/topics/<topic-slug>/03-implementation.md`.

If observed behavior contradicts the research or model, stop, document the contradiction, and return to the appropriate earlier phase for agreement. Do not patch around misunderstood semantics.

### Phase 3 gate

Present:

- What changed.
- What was actually run.
- Evidence for each acceptance criterion.
- UI link and identifiers.
- Remaining limitations or failed experiments.

Wait for the user to inspect the run and explicitly begin the grill/review.

## Phase 4: Grill and review

Create `docs/topics/<topic-slug>/04-review.md` as the durable summary.

Guide a rigorous review covering:

- Explain the pattern from first principles.
- Why this design was chosen.
- Why the main alternatives were not chosen.
- Which guarantees come from Temporal.
- Which guarantees come from our code and infrastructure.
- Replay and determinism implications.
- Retry and idempotency implications.
- Failure and cancellation behavior.
- Scaling, memory, latency, and Workflow-history implications.
- How to observe and debug it.
- How to test it.
- How the design changes under different requirements.
- Production hardening that the teaching example omits.

Ask scenario-based questions and encourage the user to predict behavior before showing the answer. Use the actual Workflow event history and code from Phase 3 as evidence.

Finish with:

- Agreed mental model.
- Rules of thumb.
- Misconceptions corrected.
- Remaining questions.
- Follow-up experiments.
- Candidate next topic from the repository `README.md`.

Do not automatically begin the next topic.

## Artifact hygiene

Keep the four phase documents focused and cumulative:

- `01-research.md`: sourced facts, patterns, alternatives, recommendation.
- `02-model.md`: agreed example and acceptance criteria.
- `03-implementation.md`: code map, commands, run evidence, UI identifiers.
- `04-review.md`: final mental model, tradeoffs, and follow-ups.

When later evidence changes an earlier conclusion, update the earlier artifact and add a short dated correction note rather than allowing contradictory documentation to remain.
