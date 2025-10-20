# Navigator Supply Chain Lakehouse - Databricks CLI

This project provides a complete data lakehouse solution for supply chain analytics using Databricks and Unity Catalog, managed through the Databricks CLI.

## 🚀 Quick Start

### Prerequisites

1. **Databricks Workspace** with Unity Catalog enabled
2. **Databricks CLI** installed and configured
3. **Python 3.11+** locally
4. **jq** for JSON processing (recommended)

### Installation

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure

# Install project dependencies
pip install -r requirements.txt

# Install jq (optional, for better output formatting)
# Windows: choco install jq
# macOS: brew install jq
# Ubuntu: sudo apt-get install jq
```

### Deploy and Run

```bash
# 1. Deploy all jobs to Databricks
./scripts/deploy.sh

# 2. Setup the environment
./scripts/run.sh setup-environment

# 3. Simulate sample data
./scripts/run.sh simulate-data

# 4. Run the data pipeline
./scripts/run.sh data-pipeline
```

## 📁 Project Structure

```
scap/
├── .databricks/
│   ├── jobs/                    # Job definition files
│   │   ├── setup-environment.json
│   │   ├── simulate-data.json
│   │   └── data-pipeline.json
│   └── job-ids/                 # Job ID storage
├── scripts/                     # Management scripts
│   ├── deploy.sh               # Deploy jobs
│   ├── run.sh                  # Run jobs
│   ├── status.sh               # Check job status
│   ├── list-jobs.sh            # List all jobs
│   └── delete.sh               # Delete jobs
├── src/                        # Source code
│   ├── cli/                    # CLI modules
│   ├── lib/                    # Utility libraries
│   ├── models/                 # Data models
│   └── pipelines/              # Data pipeline code
├── docs/                       # Documentation
└── requirements.txt            # Python dependencies
```

## 🔧 Management Scripts

### Deploy Jobs

```bash
# Deploy all jobs
./scripts/deploy.sh

# Deploy specific job
./scripts/deploy.sh --job setup-environment

# Deploy with custom parameters
./scripts/deploy.sh --job data-pipeline --params '{"stage": "bronze"}'
```

### Run Jobs

```bash
# Run setup environment
./scripts/run.sh setup-environment

# Run data simulation
./scripts/run.sh simulate-data

# Run data pipeline
./scripts/run.sh data-pipeline

# Run with custom parameters
./scripts/run.sh simulate-data --params '{"domain": "supplier", "record_count": 1000}'
```

### Monitor Jobs

```bash
# List all jobs
./scripts/list-jobs.sh

# Check specific job status
./scripts/status.sh setup-environment

# Get job run details
databricks runs get --run-id <run-id>

# Get job logs
databricks runs get-output --run-id <run-id>
```

### Delete Jobs

```bash
# Delete specific job
./scripts/delete.sh setup-environment

# Delete all jobs
./scripts/delete.sh all
```

## 📊 Data Architecture

### Bronze Layer (Raw Data)
- **Location**: `/Volumes/main/default/navigator_supply_chain/bronze/`
- **Purpose**: Store raw, unprocessed data
- **Data Sources**: Supplier, shipment, inventory data

### Silver Layer (Cleaned Data)
- **Location**: `/Volumes/main/default/navigator_supply_chain/silver/`
- **Purpose**: Cleaned, validated, and conformed data
- **Features**: Data quality checks, schema validation

### Gold Layer (Analytics Ready)
- **Location**: `/Volumes/main/default/navigator_supply_chain/gold/`
- **Purpose**: Business-ready analytics tables
- **Features**: Unified views, performance metrics, KPIs

## 🔍 Query Examples

### Unified Supply Chain View

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

## 🛠️ Development

### Local Development

```bash
# Run setup locally (requires Spark)
python setup_databricks.py --verbose

# Run with custom parameters
python setup_databricks.py --skip-sample-data --verbose
```

### Testing

```bash
# Test job deployment
./scripts/deploy.sh --job setup-environment

# Test job execution
./scripts/run.sh setup-environment

# Check job status
./scripts/status.sh setup-environment
```

## 🔧 Configuration

### Environment Variables

```bash
# Databricks configuration
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"

# Or use databricks configure
databricks configure
```

### Job Parameters

Jobs can be customized using JSON parameters:

```bash
# Custom simulation parameters
./scripts/run.sh simulate-data --params '{
  "domain": "supplier",
  "record_count": 5000,
  "output_path": "/Volumes/main/default/navigator_supply_chain/bronze/supplier"
}'

# Custom pipeline parameters
./scripts/run.sh data-pipeline --params '{
  "stage": "silver",
  "quality_checks": true,
  "publish_gate": true
}'
```

## 📈 Monitoring

### Job Status

```bash
# Check all jobs
./scripts/status.sh

# Check specific job
./scripts/status.sh data-pipeline

# Get run details
databricks runs get --run-id <run-id>
```

### Logs

```bash
# Get job logs
databricks runs get-output --run-id <run-id>

# Get specific log type
databricks runs get-output --run-id <run-id> --logs-only
```

## 🚨 Troubleshooting

### Common Issues

1. **Job Creation Fails**
   - Check Unity Catalog permissions
   - Verify serverless compute is enabled
   - Ensure artifact location is writable

2. **Job Run Fails**
   - Check job logs: `databricks runs get-output --run-id <run-id>`
   - Verify dependencies are installed
   - Check Unity Catalog table permissions

3. **Permission Errors**
   - Ensure user has `USE CATALOG` and `USE SCHEMA` permissions
   - Verify volume access permissions
   - Check job execution permissions

### Debug Commands

```bash
# Check job configuration
databricks jobs get --job-id <job-id> | jq '.settings'

# Check run state
databricks runs get --run-id <run-id> | jq '.state'

# Get detailed logs
databricks runs get-output --run-id <run-id> --logs-only

# Check workspace permissions
databricks workspace list /Shared
```

## 📚 Documentation

- [Databricks CLI Deployment Guide](docs/databricks_deployment_guide.md)
- [Architecture Overview](docs/ARCHITECTURE.md)
- [API Documentation](specs/master/contracts/openapi.yaml)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with `./scripts/deploy.sh` and `./scripts/run.sh`
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For support and questions:
- Check the troubleshooting section above
- Review the documentation in the `docs/` folder
- Open an issue in the repository
