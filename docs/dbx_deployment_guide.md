# dbx Deployment Guide: Navigator Supply Chain Lakehouse

This guide explains how to deploy and run the Navigator Supply Chain Lakehouse using `dbx` (Databricks CLI).

## Prerequisites

1. **Databricks Workspace** with Unity Catalog enabled
2. **Databricks CLI** configured: `databricks configure`
3. **dbx** installed: `pip install dbx`
4. **Python 3.11+** locally

## Project Structure

```
scap/
├── .dbx/
│   ├── project.json              # dbx project configuration
│   └── conf/
│       └── deployment.yml        # Workflow definitions
├── src/                          # Source code
├── setup_databricks.py          # Entry point for environment setup
├── simulate_data.py             # Entry point for data simulation
├── run_pipeline.py              # Entry point for pipeline execution
└── requirements.txt             # Python dependencies
```

## Deployment Workflows

The project includes 4 main workflows:

### 1. `scap-uc-setup`
- **Purpose**: Setup Unity Catalog environment
- **Tasks**: `setup_unity_catalog`

### 2. `scap-supply-chain-setup`
- **Purpose**: Setup Databricks environment for supply chain platform
- **Tasks**: `setup_databricks_environment`

### 3. `scap-data-simulation`
- **Purpose**: Generate sample data for testing
- **Tasks**: 
  - `simulate_supplier_data`
  - `simulate_shipment_data`
  - `simulate_inventory_data`

### 4. `scap-data-pipeline`
- **Purpose**: Run the complete data pipeline
- **Tasks**:
  - `bronze_ingestion` (depends on: none)
  - `silver_conformance` (depends on: bronze_ingestion)
  - `gold_publish` (depends on: silver_conformance)

## Deployment Commands

### 1. Initial Deployment
```bash
# Deploy to default environment
dbx deploy

# Deploy to specific environment
dbx deploy --environment dev
dbx deploy --environment prod
```

### 2. Run Workflows
```bash
# Setup Unity Catalog
dbx execute --workflow scap-uc-setup

# Setup supply chain environment
dbx execute --workflow scap-supply-chain-setup

# Generate sample data
dbx execute --workflow scap-data-simulation

# Run complete pipeline
dbx execute --workflow scap-data-pipeline
```

### 3. Run Individual Tasks
```bash
# Run specific tasks
dbx execute --workflow scap-data-simulation --task simulate_supplier_data
dbx execute --workflow scap-data-pipeline --task bronze_ingestion
```

### 4. Monitor Execution
```bash
# List all workflows
dbx workflows list

# Get workflow details
dbx workflows get --workflow-name scap-data-pipeline

# Get run status
dbx runs get --run-id <run-id>

# List recent runs
dbx runs list --limit 10
```

## Environment Configuration

### Development Environment
```bash
# Deploy to dev
dbx deploy --environment dev

# Run in dev
dbx execute --workflow scap-data-pipeline --environment dev
```

### Production Environment
```bash
# Deploy to prod
dbx deploy --environment prod

# Run in prod
dbx execute --workflow scap-data-pipeline --environment prod
```

## Troubleshooting

### Common Issues

1. **Authentication Error**
   ```bash
   # Reconfigure Databricks CLI
   databricks configure
   ```

2. **Workflow Not Found**
   ```bash
   # Check if workflow exists
   dbx workflows list
   
   # Redeploy if needed
   dbx deploy
   ```

3. **Task Failure**
   ```bash
   # Check task logs
   dbx runs get --run-id <run-id>
   
   # Check specific task
   dbx runs get --run-id <run-id> --task-id <task-id>
   ```

### Debugging

1. **Enable Debug Logging**
   ```bash
   export DBX_DEBUG=true
   dbx execute --workflow scap-data-pipeline
   ```

2. **Check Cluster Status**
   ```bash
   # List clusters
   databricks clusters list
   
   # Get cluster details
   databricks clusters get --cluster-id <cluster-id>
   ```

## Advanced Usage

### Custom Parameters
```bash
# Run with custom parameters
dbx execute --workflow scap-data-simulation --parameters '{"record_count": 5000}'
```

### Scheduled Runs
```bash
# Create scheduled job
dbx workflows create --workflow-name scap-daily-pipeline --schedule "0 6 * * *"
```

### Multi-Environment Deployment
```bash
# Deploy to all environments
dbx deploy --all-environments

# Run in specific environment
dbx execute --workflow scap-data-pipeline --environment prod
```

## Monitoring and Alerting

### Set up Monitoring
1. **Databricks Jobs UI**: Monitor job runs in Databricks workspace
2. **Email Alerts**: Configure email notifications for job failures
3. **Slack Integration**: Set up Slack notifications for job status

### Key Metrics to Monitor
- Job success/failure rates
- Execution time per task
- Data quality scores
- Freshness metrics

## Best Practices

1. **Environment Separation**: Use different environments for dev/staging/prod
2. **Version Control**: Tag releases and track deployments
3. **Testing**: Test workflows in dev before promoting to prod
4. **Monitoring**: Set up comprehensive monitoring and alerting
5. **Documentation**: Keep deployment documentation up to date

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review Databricks documentation
3. Check project logs in Databricks workspace
4. Contact the development team
