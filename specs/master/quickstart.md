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

### 1. Setup Environment
```bash
# Setup Unity Catalog environment
python -m src.cli setup
```

### 2. Simulate Sample Data
```bash
# Generate sample data for testing
python -m src.cli simulate --domain supplier --output-path /tmp/supplier_data --record-count 1000
python -m src.cli simulate --domain shipment --output-path /tmp/shipment_data --record-count 500
python -m src.cli simulate --domain inventory --output-path /tmp/inventory_data --record-count 2000
```

### 3. Load Data to Bronze Layer
```bash
# Load simulated data to bronze tables
python -m src.cli load --domain supplier --source-path /tmp/supplier_data
python -m src.cli load --domain shipment --source-path /tmp/shipment_data
python -m src.cli load --domain inventory --source-path /tmp/inventory_data
```

### 4. Run Data Pipeline
```bash
# Run individual stages
python -m src.cli run --stage bronze
python -m src.cli run --stage silver --domain all
python -m src.cli run --stage gold

# Or run complete pipeline
python -m src.cli run --stage all
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


