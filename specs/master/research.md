# Research: Navigator Supply Chain Lakehouse

## Unknowns From Technical Context → Resolutions

- Language/Runtime specifics on Databricks: Use Python 3.11 with Databricks Runtime (DBR 15.x) and SQL. Decision aligns with Delta Lake and PySpark ecosystem.
- Data quality framework choice: Use built-in Spark SQL constraints and expectations-style checks; allow Great Expectations interop if needed.
- Lineage capture: Use Unity Catalog lineage plus MLflow for run metadata; persist dataset-level lineage references in docs.
- Deployment workflow: Use dbx for packaging and deployment to Jobs; provide CLI for local simulation.

## Best Practices Tasks → Findings

- Databricks Medallion: Bronze (raw), Silver (conformed), Gold (curated KPIs). Use Delta Lake with OPTIMIZE/ZORDER for performance on keys/date.
- Identity resolution: Deterministic crosswalks; survivorship by trusted source then latest effective date; keep audit columns (effective_start, effective_end, is_current, source_system, load_ts).
- Freshness management: Domain-level SLAs—supplier daily, logistics hourly, inventory <=5 minutes. Track freshness tables and emit alerts on breach.
- Observability: Structured logs, metrics for latency/throughput/DQ pass/fail; alerts via Databricks alerts or webhook. Block publish on critical DQ failures.

## Decisions

- Decision: Databricks + Delta Lake + PySpark/SQL pipelines with dbx deployment
  - Rationale: Native fit for medallion architecture, Delta ACID guarantees, and managed workflows
  - Alternatives considered: Snowflake + Snowpark; pure Spark on EMR; each adds ops overhead or lacks native Unity lineage

- Decision: Expectations-based DQ with publish blocking
  - Rationale: Aligns with FR-002 and FR-007; Spark-native checks are efficient at scale
  - Alternatives considered: Great Expectations only; custom validators—both viable but add tooling overhead initially

- Decision: Deterministic identity resolution with crosswalks and SCD2 for reference
  - Rationale: Meets FR-001/FR-003/FR-014; preserves auditability and time-effective queries
  - Alternatives considered: Probabilistic matching—out of scope for initial pilot; may add later

- Decision: Role-based access via Unity Catalog grants; mask sensitive attributes
  - Rationale: Meets FR-005/FR-006; least-privilege defaults
  - Alternatives considered: Application-level RBAC—unnecessary for platform data access

- Decision: Performance goal for KPI queries ~5s on curated views for validation set
  - Rationale: Supports SC-003 usability; tuned with OPTIMIZE/ZORDER
  - Alternatives considered: No explicit target—reduces accountability

## Clarifications Resolved (from spec)

- Freshness targets per domain as specified: supplier daily; logistics hourly; inventory <=5 minutes.
- Roles and default permissions: analyst, engineer, auditor, external partner.
- Retention: raw 90d; conformed 1y; lineage 90d.
- Observability minimums: structured logs; latency/throughput/DQ metrics; dataset lineage; SLA alerts.
- Identity resolution rules: deterministic with crosswalks; survivorship by trusted source + latest effective date.


