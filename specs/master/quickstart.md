# Quickstart: Navigator Supply Chain Lakehouse

## Prerequisites
- Databricks workspace with Unity Catalog enabled
- Databricks CLI configured: `databricks configure`
- Python 3.11+ locally
- Access to Unity Catalog volumes for artifact storage

## Setup
1. Clone repository locally
2. Install dependencies: `pip install -r requirements.txt`
3. Configure Databricks CLI: `databricks configure`
4. Deploy jobs to Databricks: `./scripts/deploy.sh`

## Run Pipelines (Using Databricks CLI)

### 1. Deploy Jobs to Databricks
```bash
# Deploy all jobs to Databricks
./scripts/deploy.sh

# Deploy specific job
./scripts/deploy.sh --job setup-environment

# Deploy with custom parameters
./scripts/deploy.sh --job data-pipeline --params '{"stage": "bronze"}'
```

### 2. Setup Environment
```bash
# Run the setup job
databricks jobs run-now --job-id $(cat .databricks/job-ids/setup-environment.txt)

# Or use the management script
./scripts/run.sh setup-environment
```

### 3. Simulate Sample Data
```bash
# Run data simulation job
./scripts/run.sh simulate-data

# Run with custom parameters
./scripts/run.sh simulate-data --params '{"domain": "supplier", "record_count": 1000}'
```

### 4. Run Data Pipeline
```bash
# Run complete data pipeline
./scripts/run.sh data-pipeline

# Run individual pipeline stages
./scripts/run.sh data-pipeline --params '{"stage": "bronze"}'
./scripts/run.sh data-pipeline --params '{"stage": "silver"}'
./scripts/run.sh data-pipeline --params '{"stage": "gold"}'
```

### 5. Monitor Jobs
```bash
# List all jobs
./scripts/list-jobs.sh

# Get job status
./scripts/status.sh setup-environment

# Get job run details
databricks runs get --run-id <run-id>

# Get job logs
databricks runs get-output --run-id <run-id>
```

### 6. Job Management
```bash
# Start a job
./scripts/start.sh <job-name>

# Stop a running job
./scripts/stop.sh <run-id>

# Delete a job
./scripts/delete.sh <job-name>

# Update job configuration
./scripts/update.sh <job-name>
```

## Query Examples

### Unified Supply Chain Query
```sql
-- Get unified view of supplier, inventory, and shipment data
SELECT 
    product_id,
    location_id,
    supplier_name,
    supplier_region,
    quantity_on_hand,
    safety_stock,
    stock_status,
    shipment_status,
    delivery_delay_days,
    last_data_update_ts
FROM gold.unified_supply_chain
WHERE supplier_region = 'North America'
AND inventory_date >= CURRENT_DATE() - INTERVAL 7 DAYS
ORDER BY product_id, location_id;
```

### Stockout Risk Analysis
```sql
-- Identify products at risk of stockout
SELECT 
    product_id,
    location_id,
    supplier_name,
    quantity_on_hand,
    safety_stock,
    stock_status,
    CASE 
        WHEN quantity_on_hand = 0 THEN 'IMMEDIATE'
        WHEN quantity_on_hand <= safety_stock THEN 'HIGH'
        ELSE 'LOW'
    END as risk_level
FROM gold.unified_supply_chain
WHERE stock_status IN ('at_risk', 'stockout')
ORDER BY risk_level, quantity_on_hand;
```

### Supplier Performance Metrics
```sql
-- Get supplier performance summary
SELECT 
    supplier_name,
    supplier_region,
    total_shipments,
    on_time_deliveries,
    on_time_rate,
    avg_delay_days,
    performance_score
FROM gold.supplier_performance
ORDER BY performance_score DESC;
```

### Inventory Turnover Analysis
```sql
-- Analyze inventory turnover by location
SELECT 
    location_id,
    total_items,
    total_on_hand,
    total_safety_stock,
    total_in_transit,
    ROUND(total_on_hand / NULLIF(total_safety_stock, 0), 2) as coverage_ratio
FROM gold.inventory_metrics
ORDER BY coverage_ratio DESC;
```

## Data Quality & Publication Gate
- Expectations run during silver→gold; publication blocked on critical failures.
- Check data quality status: `SELECT * FROM ops.data_quality_status`

## Freshness Monitoring
- Query freshness table: `SELECT * FROM ops.freshness_status`
- Check last update times: `SELECT * FROM gold.unified_supply_chain ORDER BY last_data_update_ts DESC LIMIT 10`

## API Contracts
- See `specs/master/contracts/openapi.yaml` for endpoints used by consumer apps.

## Troubleshooting

### Common Issues
1. **Unity Catalog not configured**: Run `python -m src.cli setup` first
2. **Data quality failures**: Check bronze data quality before running silver/gold stages
3. **Permission errors**: Ensure proper Unity Catalog permissions are set

### Debug Commands
```bash
# Check data quality
python -m src.cli run --stage silver --domain supplier

# Process late-arriving updates
python -c "
from src.pipelines.silver_jobs import run_late_arriving_updates
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('LateUpdates').getOrCreate()
run_late_arriving_updates(spark, 'all', 24)
spark.stop()
"
```


