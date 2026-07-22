# Temporal topic labs

Each topic from the repository root `README.md` moves through four explicit, user-approved phases:

1. `01-research.md` — problem definition, industry patterns, Temporal semantics, alternatives, tradeoffs, sources, and recommendation.
2. `02-model.md` — agreed teaching example, diagrams, failure behavior, tests, and acceptance criteria.
3. `03-implementation.md` — code map, commands, runtime evidence, Workflow/Run IDs, and Temporal UI links.
4. `04-review.md` — the grill session’s mental model, tradeoffs, rules of thumb, and follow-up experiments.

Artifacts for a topic live in a lowercase hyphenated directory:

```text
docs/topics/<topic-slug>/
├── 01-research.md
├── 02-model.md
├── 03-implementation.md
└── 04-review.md
```

The project-local skill at `.agents/skills/temporal-pattern-lab/SKILL.md` enforces the phase gates. A phase does not advance until its document has been reviewed and explicitly approved.
