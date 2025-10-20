# Implementation Plan: Navigator Supply Chain Lakehouse

**Branch**: `master` | **Date**: 2025-10-20 | **Spec**: `C:\projects\scap\specs\master\spec.md`
**Input**: Feature specification from `/specs/master/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/commands/plan.md` for the execution workflow.

## Summary

Build a Databricks-based, multi-source supply chain data platform that unifies
supplier, logistics, and inventory data into a conformed analytics layer with
deterministic identity resolution and strong governance/observability. Initial
approach: use Delta Lake tables with medallion architecture (raw → conformed →
curated), PySpark/SQL pipelines, deterministic crosswalks for key resolution,
structured logging and metrics, and role-based access controls for governed
outputs. Freshness targets: supplier daily, logistics hourly, inventory near-
real-time (<= 5 minutes).

## Technical Context

<!--
  ACTION REQUIRED: Replace the content in this section with the technical details
  for the project. The structure here is presented in advisory capacity to guide
  the iteration process.
-->

**Language/Version**: Python 3.11, SQL (Databricks SQL)  
**Primary Dependencies**: Databricks Runtime (PySpark), Delta Lake, dbx (deployment), great_expectations or expectations in Spark for DQ, MLflow (lineage/metrics)  
**Storage**: Delta Lake tables on Databricks (bronze/silver/gold)  
**Testing**: pytest for unit tests, data contract tests (e.g., expectations), notebook/pipeline tests via dbx  
**Target Platform**: Databricks (Jobs/Workflows), Unity Catalog-enabled workspace  
**Project Type**: single (pipelines + libraries + SQL)  
**Performance Goals**: Meet freshness SLAs; KPI queries for validation set within ~5s on curated views  
**Constraints**: Freshness: supplier daily; logistics hourly; inventory <=5 minutes; publication blocked on critical DQ failures  
**Scale/Scope**: Pilot-scope synthetic datasets representing common supply chain domains; expand to additional sources later

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Authoritative constitution: `C:\projects\scap\.specify\memory\constitution.md`

Applicable gates (from constitution):

- Test-First (NON-NEGOTIABLE): tests/contracts precede implementation; Red‑Green‑Refactor required.
- CLI/Automation: repeatable CLI/job entry points; text I/O; JSON where applicable.
- Observability: structured logs, metrics (latency/throughput/DQ), dataset lineage, alerts on SLA/DQ/freshness breaches.
- Simplicity: single-project layout; avoid premature abstractions.
- Versioning & Breaking Changes: version schemas/curated views; document migrations.

Status: Planned with no violations at this stage; will re-evaluate after Phase 1 design.

## Project Structure

### Documentation (this feature)

```
specs/[###-feature]/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)
<!--
  ACTION REQUIRED: Replace the placeholder tree below with the concrete layout
  for this feature. Delete unused options and expand the chosen structure with
  real paths (e.g., apps/admin, packages/something). The delivered plan must
  not include Option labels.
-->

```
src/
├── pipelines/                  # PySpark/SQL jobs (bronze/silver/gold)
│   ├── sql/
│   │   ├── unified_view.sql
│   │   ├── kpis.sql
│   │   ├── freshness_fn.sql
│   │   └── dq_status_fn.sql
│   ├── gold_jobs.py
│   ├── gold_kpi_jobs.py
│   ├── silver_jobs.py
│   ├── bronze_jobs.py
│   ├── maintenance.py          # OPTIMIZE/ZORDER routines
│   └── security/               # Grants and masking SQL
│       └── grants.sql
├── models/                     # Reusable business logic (keys, transforms)
│   ├── crosswalk.py
│   ├── identity_resolution.py
│   ├── supplier.py             # SCD2
│   ├── shipment.py
│   └── inventory_position.py
├── cli/                        # CLI entry points for local simulations
│   └── __init__.py
└── lib/                        # Utilities (logging, dq helpers, publish gate, lineage)
    ├── logging.py
    ├── dq.py
    ├── publish_gate.py
    └── lineage.py

tests/
├── contract/                   # Data contract + schema tests
├── integration/                # End-to-end pipeline validations
└── unit/                       # Pure Python unit tests
```

**Structure Decision**: Single-project layout optimized for Databricks pipelines
and contracts. Documentation and planning artifacts for this run are under
`C:\projects\scap\specs\master\`.

## Non-Functional Performance Target

For the validation dataset sized to the provided synthetic inputs and on the
baseline Databricks cluster for this project, curated KPI queries MUST achieve
P95 latency ≤ 5 seconds. Verification will be implemented via automated query
timing in CI against the validation dataset.

## Complexity Tracking

*Fill ONLY if Constitution Check has violations that must be justified*

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| [e.g., 4th project] | [current need] | [why 3 projects insufficient] |
| [e.g., Repository pattern] | [specific problem] | [why direct DB access insufficient] |

