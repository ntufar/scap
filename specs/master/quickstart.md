# Quickstart: Navigator Supply Chain Lakehouse

## Prerequisites
- Databricks workspace with Unity Catalog
- Python 3.11 locally
- dbx installed (`pip install dbx`)

## Setup
1. Clone repository and create virtual env
2. Configure Databricks CLI and set workspace host/token
3. Create catalogs/schemas for bronze, silver, gold

## Run Pipelines (Local Simulation)
```bash
python -m src.cli.load_sample_data --domain supplier
python -m src.cli.run_pipeline --stage bronze
python -m src.cli.run_pipeline --stage silver
python -m src.cli.run_pipeline --stage gold
```

## Data Quality & Publication Gate
- Expectations run during silver→gold; publication blocked on critical failures.

## Freshness Monitoring
- Query freshness table: `select * from ops.freshness_status`.

## API Contracts
- See `specs/master/contracts/openapi.yaml` for endpoints used by consumer apps.


