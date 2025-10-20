# Feature Specification: Navigator Supply Chain Lakehouse

**Feature Branch**: `supply-chain-lakehouse`  
**Created**: 2025-10-20  
**Status**: Draft  
**Input**: User description: "Build a multi-source supply chain data platform using Databricks that demonstrates the key technical capabilities needed for Navigator 2.0. This simulates integrating supplier, logistics, and inventory data into a unified analytics layer." (derived from `docs/PSD.md`)

## Clarifications

### Session 2025-10-20

- Q: Target freshness per domain? → A: Supplier daily; logistics hourly; inventory near-real-time (<=5 minutes)
- Q: Access role taxonomy and default permissions? → A: Four roles — analyst (read curated), engineer (dev+ops), auditor (read lineage/QC), external partner (limited curated)
- Q: Retention and audit policy for historical data? → A: Raw 90 days; conformed 1 year; lineage retained 90 days
- Q: Observability signals for pipelines (minimum set)? → A: Structured logs; metrics for latency/throughput/DQ pass-fail; dataset-level lineage; alerts on SLA breaches
- Q: Identity resolution rules for supplier and product keys? → A: Deterministic rules with explicit crosswalks and survivorship by trusted source + latest effective date

## User Scenarios & Testing (mandatory)

### User Story 1 - Unified Supply Chain Data (Priority: P1)

As a supply chain analyst, I need a unified, trustworthy analytics layer combining
supplier, logistics, and inventory data so I can compute KPIs and investigate
disruptions without manual reconciliation.

**Why this priority**: Establishes the foundational capability; all downstream
analytics depend on trusted, unified data.

**Independent Test**: A single SQL query can retrieve synchronized supplier,
shipment, and inventory records for a given product and date range with no
orphan references or duplicate business keys.

**Acceptance Scenarios**:
1. Given simulated supplier, logistics, and inventory inputs, When data is
   ingested and conformed, Then the unified view exposes consistent product,
   location, and supplier keys with no duplicates for the same natural key and
   effective date.
2. Given late-arriving logistics updates, When the next load runs, Then the
   conformed layer reflects corrected shipments without breaking historical
   integrity.

---

### User Story 2 - Supply Chain KPI Insights (Priority: P2)

As an operations manager, I need curated metrics for lead times, capacity, and
inventory sufficiency so I can track performance and take corrective actions.

**Why this priority**: Delivers visible business value and executive reporting.

**Independent Test**: KPI queries return within target thresholds and reconcile
to known test scenarios (e.g., synthetic cases).

**Acceptance Scenarios**:
1. Given conformed data, When querying average lead time by supplier and route,
   Then results match expected computations for a prepared validation set.
2. Given inventory and demand inputs, When computing sufficiency status, Then
   items with projected shortfalls are flagged with consistent thresholds.

---

### User Story 3 - Governance & Observability (Priority: P3)

As a platform steward, I need data quality checks, lineage, and access controls
so that the platform is compliant, auditable, and resilient to data issues.

**Why this priority**: Ensures trust, safety, and sustainable operations.

**Independent Test**: Quality violations block publication; lineage shows source
to consumption; access policies restrict non-authorized roles.

**Acceptance Scenarios**:
1. Given a schema violation in incoming data, When the pipeline runs, Then the
   publish step is blocked and a clear error state is reported.
2. Given a non-authorized user, When attempting to read governed outputs, Then
   access is denied and logged.

---

### Edge Cases

- Late-arriving events change historical shipments; ensure corrections do not
  duplicate or lose records in time-aware views.
- Conflicting supplier identifiers across sources; ensure robust key mapping and
  survivorship rules.
- If conflicting attributes exist for the same business key, apply trusted
  source precedence then latest effective date; retain audit trail of prior
  values.
- Sparse or missing inventory updates; ensure freshness thresholds and fallback
  logic for KPI calculations are well-defined.

## Requirements (mandatory)

### Functional Requirements

- FR-001: The platform MUST unify supplier, logistics, and inventory inputs into
  a conformed analytics layer with consistent business keys.
- FR-002: The system MUST validate incoming data against documented schemas and
  reject or quarantine records that violate constraints.
- FR-003: The system MUST maintain history for changing reference data (e.g.,
  supplier attributes) and support time-effective queries.
- FR-004: The system MUST provide lead time, capacity, and sufficiency metrics
  that are accurate for a prepared validation dataset.
- FR-005: The system MUST expose governance artifacts: data dictionary, lineage
  references, and access rules per role category.
- FR-006: The system MUST protect sensitive attributes using role-based access
  and masked outputs where required.
- FR-007: The platform MUST surface data quality status and block publication of
  outputs when critical checks fail.
- FR-008: The platform MUST provide queryable status of data freshness per
  domain (supplier, logistics, inventory).
- FR-009: The platform MUST allow analysts to retrieve unified records for a
  given product, location, supplier, and time window with deterministic results.
- FR-010: The system MUST provide a clear runbook for common operational
  failures (ingestion delays, schema drift, data gaps) and a path to recovery.

- FR-011: Target freshness by domain MUST be: supplier daily; logistics hourly;
  inventory near-real-time (<=5 minutes) with status visibility per domain.
  This requirement is authoritative for freshness targets; earlier clarifications
  reference this source of truth.

- FR-012: Access role taxonomy MUST include: analyst (read curated datasets);
  engineer (development and operations management); auditor (read lineage and
  quality/control artifacts); external partner (limited read to curated
  exports). Defaults follow least-privilege; sensitive attributes are masked per
  role.

- FR-013: Data retention MUST follow: raw data retained 90 days; conformed
  outputs retained 1 year; lineage and audit logs retained 90 days; deletions
  are secure and compliant with policy.

- FR-014: Identity resolution MUST be deterministic: exact match on natural
  keys; explicit crosswalk mappings across sources; survivorship determined by
  trusted source precedence and latest effective date; all merges auditable.

- FR-015: Observability MUST include structured logs; metrics for latency,
  throughput, and data quality pass/fail; dataset-level lineage; and alerts on
  SLA/freshness and DQ breaches via an alerting channel (e.g., Databricks Jobs
  alerts or webhook). Acceptance: alerts are emitted within 10 minutes of breach
  detection and recorded in logs/metrics.

### Key Entities (include if feature involves data)

- Supplier: natural key, business key, attributes, effective dating.
- Shipment: shipment id, route, transit timestamps, status, carrier.
- InventoryPosition: item, location, quantity on hand, date, safety stock.
- CostReference: cost dimension attributes for analysis joins.

## Success Criteria (mandatory)

### Measurable Outcomes

- SC-001: 95% of unified queries for a target validation set return correct
  joins with zero orphan keys.
- SC-002: 100% of critical data quality violations block publication of the
  affected outputs and produce actionable error messages.
- SC-003: KPI queries for the validation set produce expected values within 1%
  tolerance of hand-calculated results.
- SC-004: Freshness status for each domain is visible, and breaches are flagged
  within 10 minutes of detection via configured alerting channel and dashboard
  indicator.
- SC-005: Access controls prevent non-authorized users from reading governed
  outputs in 100% of tested cases.
- SC-006: Observability metrics capture pipeline latency/throughput and DQ
  pass/fail, and alerts are raised for SLA breaches in test scenarios.

## Assumptions

- Simulated datasets are representative of common enterprise supply chain data
  fields and quality characteristics.
- Validation datasets and expected KPI values will be provided or derived from
  synthetic cases to enable objective testing.
- The platform will operate with separate environments for development and
  production-style validation before release.

## Clarifications Needed (max 3)

1. Confirm alerting channel preference (email, webhook, Slack) for SLA/DQ breaches.
2. Confirm baseline cluster configuration used for KPI latency measurement.
3. Confirm retention enforcement mechanism (Unity Catalog retention vs VACUUM/DELETE policy).
