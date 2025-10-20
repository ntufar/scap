# Project Constitution

## Core Principles

### I. Test-First (NON-NEGOTIABLE)
- MUST write tests and/or data contracts before implementation (Red-Green-Refactor).
- MUST include contract tests for schemas and curated views before publishing breaking changes.
- SHOULD maintain fast-running unit tests and representative integration tests in CI.

### II. CLI/Automation
- MUST provide repeatable CLI or job entry points for all workflows.
- MUST use text I/O conventions: stdin/args → stdout; errors → stderr.
- SHOULD support JSON output for machine consumption where applicable.

### III. Observability
- MUST emit structured logs and metrics (latency, throughput, DQ pass/fail) for all pipelines.
- MUST record dataset-level lineage for silver→gold transitions.
- MUST raise alerts on SLA/freshness/DQ breaches via configured channel.

### IV. Simplicity
- MUST prefer a single-project layout unless complexity is justified.
- SHOULD avoid premature abstractions; refactor only when demanded by tests/usage.

### V. Versioning & Breaking Changes
- MUST version schemas and curated views; document migrations for breaking changes.
- MUST provide a migration note in `CHANGELOG` or equivalent when contracts change.

## Quality Gates
- MUST pass Constitution Check before Phase 0 and re-check after Phase 1.
- MUST block publication on critical DQ failures.
- MUST verify role-based access controls and masking in a dedicated test pass.

## Governance
- Constitution supersedes other practices; conflicts must be resolved by adjusting specs/plans/tasks.
- Amendments require documentation, approval, and a migration plan if applicable.

**Version**: 1.0.0 | **Ratified**: 2025-10-20 | **Last Amended**: 2025-10-20
