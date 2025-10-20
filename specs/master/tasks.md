# Tasks: Navigator Supply Chain Lakehouse

## Phase 1 — Setup

- [X] T001 Initialize single-project structure per plan.md (src/, tests/) at repo root
- [X] T002 Configure Databricks CLI and dbx project scaffolding in `src/` and `.dbx/`
- [X] T003 Create Unity Catalog objects (catalogs/schemas) for bronze, silver, gold
- [X] T004 Add Python 3.11 env and dependencies (pyspark, delta-spark, dbx, mlflow) in `requirements.txt`
- [X] T005 Create logging utility in src/lib/logging.py
- [X] T006 Create DQ helper in src/lib/dq.py

## Phase 2 — Foundational (blocking for all stories)

- [ ] T007 Implement crosswalk model/table definitions in src/models/crosswalk.py
- [ ] T008 Implement identity resolution functions in src/models/identity_resolution.py
- [ ] T009 Implement freshness status table and updater in src/models/freshness.py
- [ ] T010 Implement publish gate (block on critical DQ) in src/lib/publish_gate.py
 - [ ] T010a Implement bronze-level schema validation at ingestion (contracts + enforcement)

## Phase 3 — User Story 1 (P1) Unified Supply Chain Data

Goal: Unified conformed analytics layer; deterministic joins across supplier, logistics, and inventory.
Independent Test: Single SQL query returns synchronized supplier, shipment, inventory for product/date with zero orphan keys.

- [ ] T011 [US1] Create Supplier SCD2 model in src/models/supplier.py
- [ ] T012 [P] [US1] Create Shipment model in src/models/shipment.py
- [ ] T013 [P] [US1] Create InventoryPosition model in src/models/inventory_position.py
- [ ] T014 [US1] Implement bronze ingestion jobs in src/pipelines/bronze_jobs.py
- [ ] T015 [US1] Implement silver conformance (keys, crosswalks, SCD2) in src/pipelines/silver_jobs.py
- [ ] T016 [US1] Create unified view SQL in src/pipelines/sql/unified_view.sql
- [ ] T017 [US1] Implement late-arriving updates handling in silver logic in src/pipelines/silver_jobs.py
- [ ] T018 [US1] Wire publish to gold tables with gate in src/pipelines/gold_jobs.py
- [ ] T019 [US1] CLI entry points for simulate/load/run in src/cli/__init__.py
- [ ] T020 [US1] Update quickstart steps with US1 query example in specs/master/quickstart.md

## Phase 4 — User Story 2 (P2) Supply Chain KPI Insights

Goal: Curated KPI metrics for lead times, capacity, sufficiency.
Independent Test: KPI queries match validation set; query latency within target.

- [ ] T021 [US2] Define KPI models and SQL in src/pipelines/sql/kpis.sql
- [ ] T022 [US2] Implement gold KPI jobs in src/pipelines/gold_kpi_jobs.py
- [ ] T023 [P] [US2] Add OPTIMIZE/ZORDER routines for KPI tables in src/pipelines/maintenance.py
- [ ] T024 [US2] Document KPI query examples in specs/master/quickstart.md

## Phase 5 — User Story 3 (P3) Governance & Observability

Goal: DQ checks, lineage, access controls.
Independent Test: Critical DQ failures block publish; lineage visible; RBAC enforced.

- [ ] T025 [US3] Implement expectations-based checks for silver→gold in src/lib/dq.py
- [ ] T026 [US3] Emit structured logs and metrics in src/lib/logging.py
- [ ] T027 [P] [US3] Implement lineage metadata capture in src/lib/lineage.py
- [ ] T028 [US3] Define Unity Catalog grants and masks in src/pipelines/security/grants.sql
- [ ] T029 [US3] Expose freshness endpoint contract via notebook/SQL function in src/pipelines/sql/freshness_fn.sql
 - [ ] T029a [US3] Implement alerting for SLA/freshness/DQ breaches (Databricks alerts/integration)

## Phase 6 — Contracts Alignment

- [ ] T030 Align unified-records contract to curated views in src/pipelines/sql/unified_view.sql
- [ ] T031 Implement freshness status contract producer in src/pipelines/sql/freshness_fn.sql
- [ ] T032 Implement DQ status contract producer in src/pipelines/sql/dq_status_fn.sql
 - [ ] T032a Generate and publish data dictionary from schemas/Unity Catalog comments to docs/

## Final Phase — Polish & Cross-Cutting

- [ ] T033 Add runbook for ops failures in docs/runbook.md
- [ ] T034 Add sample validation dataset and expected KPI values in docs/validation/
- [ ] T035 Add CI workflow for contract/integration checks in .github/workflows/ci.yml
 - [ ] T036 Add retention enforcement (VACUUM/TTL policies, deletion workflow) and tests
 - [ ] T037 Document role taxonomy and default grants/masks mapping in docs/governance/roles.md
 - [ ] T038 Verify KPI performance target in CI (P95 ≤5s) with automated timing

## Dependencies (Story Order)

US1 → US2 → US3

## Parallel Execution Examples

- T012 and T013 in parallel (distinct models)
- T023 and T024 in parallel (maintenance vs docs)
- T027 and T028 in parallel (lineage vs grants)

## Implementation Strategy

MVP: Deliver Phase 3 (US1) end-to-end with conformed layer and unified view; then iterate on KPIs (US2) and governance (US3).


