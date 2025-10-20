# Unity Catalog Deployment Guide

## Prerequisites

1. **Databricks Workspace** with Unity Catalog enabled
2. **Databricks CLI** configured with proper authentication
3. **Java 8 or 11** installed (for local PySpark development)

## Deployment Steps

### 1. Configure Databricks CLI

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure --token
# Enter your workspace URL and personal access token
```

### 2. Deploy Unity Catalog Objects

```bash
# Run the Unity Catalog setup script
python -m src.pipelines.setup_uc

# Or run individual SQL files in Databricks SQL
# 1. Execute src/pipelines/sql/bootstrap_uc.sql
# 2. Execute src/pipelines/sql/create_tables.sql
```

### 3. Verify Deployment

```sql
-- Check catalogs
SHOW CATALOGS;

-- Check schemas
SHOW SCHEMAS IN CATALOG bronze;
SHOW SCHEMAS IN CATALOG silver;
SHOW SCHEMAS IN CATALOG gold;

-- Check tables
SHOW TABLES IN bronze.supplier;
SHOW TABLES IN silver.conformed;
SHOW TABLES IN gold.curated;
```

## Architecture Overview

### Catalogs
- **bronze**: Raw data ingestion layer
- **silver**: Conformed data layer with business logic
- **gold**: Curated analytics layer

### Schemas by Domain
- **supplier**: Supplier master data
- **logistics**: Shipment and transportation data
- **inventory**: Inventory positions and stock levels
- **conformed**: Cross-domain conformed data
- **curated**: Business-ready analytics views
- **kpis**: Key performance indicators
- **analytics**: Advanced analytics tables

### Security Roles
- **analysts**: Read access to gold layer
- **engineers**: Full access to all layers
- **auditors**: Read access to all layers
- **external_partners**: Limited access to specific gold views

## Next Steps

1. **Data Ingestion**: Implement bronze layer jobs to load raw data
2. **Data Conformance**: Build silver layer transformations
3. **Analytics**: Create gold layer curated views and KPIs
4. **Monitoring**: Set up freshness and quality monitoring
5. **Access Control**: Configure user groups and permissions

## Troubleshooting

### Common Issues

1. **Permission Denied**: Ensure your user has catalog management permissions
2. **Schema Already Exists**: Use `IF NOT EXISTS` clauses (already included)
3. **Table Creation Fails**: Check Delta Lake configuration and storage permissions

### Validation

Run the validation script to check SQL syntax:
```bash
python -m src.pipelines.validate_uc
```

This will validate all SQL statements and show the execution plan without requiring a Databricks connection.
