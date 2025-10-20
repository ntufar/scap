<!--
Sync Impact Report
- Version change: N/A → 1.0.0
- Modified principles:
  - [PRINCIPLE_1_NAME] → Data Contracts & Schema Governance
  - [PRINCIPLE_2_NAME] → Medallion Architecture (Bronze/Silver/Gold)
  - [PRINCIPLE_3_NAME] → Data Quality as Code (Test-First)
  - [PRINCIPLE_4_NAME] → Observability, Lineage, and Auditability
  - [PRINCIPLE_5_NAME] → Security, Privacy, and Access Control
- Added sections: Additional Constraints & Tech Stack; Development Workflow & Review Process
- Removed sections: None
- Templates requiring updates:
  - .specify/templates/plan-template.md: ✅ updated (Constitution Check aligns generically)
  - .specify/templates/spec-template.md: ✅ updated (no conflicting guidance)
  - .specify/templates/tasks-template.md: ✅ updated (story-first, optional tests consistent)
  - .specify/templates/agent-file-template.md: ✅ updated (generic runtime guidance)
- Follow-up TODOs: None
-->

# Navigator 2.0 Supply Chain Lakehouse Constitution

## Core Principles

### Data Contracts & Schema Governance
All datasets that enter or exit the platform MUST be governed by explicit data
contracts: documented schemas, field-level definitions, nullability, semantic
types, and SLAs for freshness and completeness. Contracts are versioned and
validated at ingestion and before publication. Breaking changes require a new
major version and coexistence period; non-breaking changes require a minor
version and clear migration notes.

### Medallion Architecture (Bronze/Silver/Gold)
All data flows MUST implement the Databricks medallion pattern:
- Bronze: raw immutable ingestion with minimal normalization and full lineage.
- Silver: clean, conformed, CDC/SCD2 where applicable, business keys enforced.
- Gold: curated analytics models optimized for BI/ML and governed via Unity
  Catalog with clear ownership and SLAs.
Transitions between layers MUST be reproducible, idempotent, and expressed as
code (Delta, Spark SQL, or PySpark), scheduled via Workflows.

### Data Quality as Code (Test-First)
Data pipelines MUST embed test-first quality checks (e.g., expectations) that
fail fast on contract violations, freshness breaches, duplicates, referential
integrity issues, and metric drifts. Quality gates run on Bronze→Silver and
Silver→Gold transitions and block publishes on failure. Quality rules are stored
in version control and reviewed like application code.

### Observability, Lineage, and Auditability
Pipelines MUST emit structured operational telemetry (latency, throughput,
cost, failure reasons) and data telemetry (row counts, null rates, distribution
stats). Lineage MUST be available via Unity Catalog for table-to-table and
column-level where supported. All access, changes, and deployments are audited
and retained per compliance requirements.

### Security, Privacy, and Access Control
Unity Catalog MUST enforce RBAC with least privilege across catalogs/schemas.
PII and sensitive fields MUST be tagged and protected with masking or row-level
filters as required. Secrets are managed via Databricks secrets or cloud KMS.
All inter-system interfaces use secure endpoints and encrypted at rest/in
transit. Production changes require review and environment promotion controls.

## Additional Constraints & Tech Stack

- Platform: Databricks Lakehouse (Delta Lake, Unity Catalog, Workflows).
- Ingestion: Auto Loader for files/streams; connectors for databases with CDC.
- Storage: Delta tables with OPTIMIZE/VACUUM policies; Z-Ordering where useful.
- Processing: Spark SQL/PySpark; streaming where latency ≤ near-real-time is
  required; batch for daily aggregates.
- Modeling: Business entities for suppliers, logistics, inventory; SCD2 for
  dimension changes; fact tables for movements and stock.
- Integration: Supplier, logistics, and inventory data unified in a conformed
  analytics layer; publish via SQL endpoints or warehouse.
- Performance/Cost: p95 job runtime SLOs per pipeline; auto-scaling clusters;
  cost budgets with alerts.
- SLAs: Freshness SLAs defined per domain (e.g., supplier daily, logistics
  hourly, inventory near-real-time); incidents tracked and reviewed.

## Development Workflow & Review Process

- Everything as Code: pipelines, expectations, tables, and cluster/workflow
  configs live in version control; infra changes via IaC (e.g., Terraform
  provider for Databricks) where feasible.
- Reviews: PRs MUST include data contract diffs, quality checks, lineage
  impacts, and backfill/rollback plan.
- Environments: Dev → Staging → Prod with artifact promotion; no direct edits in
  Prod. Changes are replayable and idempotent.
- Versioning: Semantic versioning for contracts and public Gold tables; publish
  deprecation timelines and migration guides.
- Releases: Workflows define deployment order Bronze→Silver→Gold; health checks
  gate completion.

## Governance

- Supremacy: This constitution governs architecture, data quality, security,
  and delivery practices for Navigator 2.0 supply chain analytics.
- Amendments: Proposals via PR modifying this file with rationale, risk
  assessment, migration plan, and expected user impact. Require approval from
  data platform owner and domain data owners.
- Versioning Policy: Semantic versioning for this constitution. MAJOR for
  incompatible governance changes, MINOR for principle additions/expansions,
  PATCH for clarifications.
- Compliance: All PRs MUST attest Constitution Check in plans. Regular reviews
  verify SLAs, costs, lineage completeness, and security posture. Exceptions
  require time-bound waivers and tracking.

**Version**: 1.0.0 | **Ratified**: 2025-10-20 | **Last Amended**: 2025-10-20
