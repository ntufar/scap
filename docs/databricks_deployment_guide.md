# Databricks CLI Deployment Guide

This guide explains how to deploy and manage the Navigator Supply Chain Lakehouse using the Databricks CLI instead of dbx.

## Prerequisites

1. **Databricks Workspace** with Unity Catalog enabled
2. **Databricks CLI** installed and configured
3. **Python 3.11+** locally
4. **jq** for JSON processing (optional but recommended)

### Installation

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure

# Install jq (optional, for better output formatting)
# On Windows with Chocolatey:
choco install jq
# On macOS with Homebrew:
brew install jq
# On Ubuntu/Debian:
sudo apt-get install jq
```

## Project Structure

```
scap/
├── .databricks/
│   ├── jobs/                    # Job definition files
│   │   ├── setup-environment.json
│   │   ├── simulate-data.json
│   │   └── data-pipeline.json
│   └── job-ids/                 # Job ID storage
│       ├── setup-environment.txt
│       ├── simulate-data.txt
│       └── data-pipeline.txt
├── scripts/                     # Management scripts
│   ├── deploy.sh
│   ├── run.sh
│   ├── status.sh
│   ├── list-jobs.sh
│   └── delete.sh
├── src/                        # Source code
└── requirements.txt
```

## Job Definitions

### 1. Setup Environment Job (`setup-environment.json`)

- **Purpose**: Initialize Unity Catalog environment and create sample data
- **Type**: Serverless Python wheel task
- **Entry Point**: `setup_uc`
- **Dependencies**: `databricks-sdk`

### 2. Simulate Data Job (`simulate-data.json`)

- **Purpose**: Generate simulated supplier, shipment, and inventory data
- **Type**: Multi-task serverless job
- **Tasks**: 
  - `simulate_supplier_data`
  - `simulate_shipment_data` (depends on supplier data)
  - `simulate_inventory_data` (depends on supplier data)
- **Dependencies**: `databricks-sdk`, `faker`

### 3. Data Pipeline Job (`data-pipeline.json`)

- **Purpose**: Process data through bronze → silver → gold layers
- **Type**: Multi-task serverless job
- **Tasks**:
  - `bronze_ingestion`
  - `silver_conformance` (depends on bronze)
  - `gold_publish` (depends on silver)
- **Dependencies**: `databricks-sdk`, `great-expectations`

## Management Scripts

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

## Manual Databricks CLI Commands

### Job Management

```bash
# Create a job
databricks jobs create --json-file .databricks/jobs/setup-environment.json

# Get job details
databricks jobs get --job-id <job-id>

# Update job
databricks jobs reset --job-id <job-id> --json-file .databricks/jobs/setup-environment.json

# Delete job
databricks jobs delete --job-id <job-id>

# List all jobs
databricks jobs list
```

### Run Management

```bash
# Start a job run
databricks jobs run-now --job-id <job-id>

# Start with parameters
databricks jobs run-now --job-id <job-id> --json '{"parameters": ["--stage", "bronze"]}'

# Get run details
databricks runs get --run-id <run-id>

# Get run output/logs
databricks runs get-output --run-id <run-id>

# List runs for a job
databricks runs list --job-id <job-id>

# Cancel a run
databricks runs cancel --run-id <run-id>
```

## Environment Configuration

### Serverless Environment

All jobs use serverless compute with the following configuration:

```json
{
  "environment_key": "default",
  "name": "default",
  "spec": {
    "client": "1",
    "dependencies": [
      "pypi://databricks-sdk"
    ]
  }
}
```

### Unity Catalog Integration

Jobs are configured to work with Unity Catalog volumes:

- **Bronze Layer**: `/Volumes/main/default/navigator_supply_chain/bronze/`
- **Silver Layer**: `/Volumes/main/default/navigator_supply_chain/silver/`
- **Gold Layer**: `/Volumes/main/default/navigator_supply_chain/gold/`

## Troubleshooting

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

## Migration from dbx

If migrating from dbx:

1. **Backup existing jobs**: Export job configurations
2. **Deploy new jobs**: Use `./scripts/deploy.sh`
3. **Test jobs**: Run each job individually
4. **Update documentation**: Update any references to dbx commands
5. **Clean up**: Remove dbx configuration files

## Best Practices

1. **Version Control**: Keep job definitions in version control
2. **Environment Separation**: Use different job names for different environments
3. **Monitoring**: Set up alerts for job failures
4. **Logging**: Use structured logging in your Python code
5. **Testing**: Test jobs in development before production deployment

## Security Considerations

1. **Secrets Management**: Use Databricks secrets for sensitive data
2. **Access Control**: Implement proper Unity Catalog permissions
3. **Network Security**: Configure VPC endpoints if needed
4. **Audit Logging**: Enable audit logs for compliance

## Performance Optimization

1. **Serverless Compute**: Use serverless for cost efficiency
2. **Parallel Execution**: Configure appropriate concurrency limits
3. **Resource Allocation**: Monitor and adjust timeout settings
4. **Data Partitioning**: Optimize data layout for query performance
