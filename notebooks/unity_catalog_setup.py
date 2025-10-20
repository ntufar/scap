# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Setup for Supply Chain Analytics Platform
# MAGIC 
# MAGIC This notebook sets up the Unity Catalog objects for the supply chain lakehouse architecture.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import required libraries

# COMMAND ----------

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog Bootstrap
# MAGIC 
# MAGIC Create catalogs and schemas for the medallion architecture

# COMMAND ----------

# Create catalogs for each medallion layer
spark.sql("CREATE CATALOG IF NOT EXISTS bronze")
spark.sql("CREATE CATALOG IF NOT EXISTS silver") 
spark.sql("CREATE CATALOG IF NOT EXISTS gold")
spark.sql("CREATE CATALOG IF NOT EXISTS ops")

# COMMAND ----------

# Create schemas for organized data domains
# Bronze layer schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze.raw")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze.supplier")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze.logistics")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze.inventory")

# Silver layer schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS silver.conformed")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver.supplier")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver.logistics")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver.inventory")

# Gold layer schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS gold.curated")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold.kpis")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold.analytics")

# Operations schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS ops.freshness")
spark.sql("CREATE SCHEMA IF NOT EXISTS ops.lineage")
spark.sql("CREATE SCHEMA IF NOT EXISTS ops.quality")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Permissions
# MAGIC 
# MAGIC Set up role-based access control

# COMMAND ----------

# Analyst role - read access to gold layer
spark.sql("GRANT USE CATALOG ON CATALOG gold TO `analysts`")
spark.sql("GRANT USE SCHEMA ON SCHEMA gold.curated TO `analysts`")
spark.sql("GRANT USE SCHEMA ON SCHEMA gold.kpis TO `analysts`")
spark.sql("GRANT USE SCHEMA ON SCHEMA gold.analytics TO `analysts`")
spark.sql("GRANT SELECT ON SCHEMA gold.curated TO `analysts`")
spark.sql("GRANT SELECT ON SCHEMA gold.kpis TO `analysts`")
spark.sql("GRANT SELECT ON SCHEMA gold.analytics TO `analysts`")

# COMMAND ----------

# Engineer role - full access to all layers
spark.sql("GRANT USE CATALOG ON CATALOG bronze TO `engineers`")
spark.sql("GRANT USE CATALOG ON CATALOG silver TO `engineers`")
spark.sql("GRANT USE CATALOG ON CATALOG gold TO `engineers`")
spark.sql("GRANT ALL PRIVILEGES ON CATALOG bronze TO `engineers`")
spark.sql("GRANT ALL PRIVILEGES ON CATALOG silver TO `engineers`")
spark.sql("GRANT ALL PRIVILEGES ON CATALOG gold TO `engineers`")

# COMMAND ----------

# Auditor role - read access to all layers for compliance
spark.sql("GRANT USE CATALOG ON CATALOG bronze TO `auditors`")
spark.sql("GRANT USE CATALOG ON CATALOG silver TO `auditors`")
spark.sql("GRANT USE CATALOG ON CATALOG gold TO `auditors`")
spark.sql("GRANT SELECT ON CATALOG bronze TO `auditors`")
spark.sql("GRANT SELECT ON CATALOG silver TO `auditors`")
spark.sql("GRANT SELECT ON CATALOG gold TO `auditors`")

# COMMAND ----------

# External partner role - limited access to specific gold views
spark.sql("GRANT USE CATALOG ON CATALOG gold TO `external_partners`")
spark.sql("GRANT USE SCHEMA ON SCHEMA gold.curated TO `external_partners`")
spark.sql("GRANT SELECT ON SCHEMA gold.curated TO `external_partners`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Tables
# MAGIC 
# MAGIC Create the core tables for the supply chain lakehouse

# COMMAND ----------

# Bronze layer tables (raw data)
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.supplier.raw_suppliers (
    natural_key STRING,
    source_system STRING,
    raw_data STRING,
    load_ts TIMESTAMP,
    file_name STRING
) USING DELTA
COMMENT 'Raw supplier data from source systems'
""")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.logistics.raw_shipments (
    shipment_id STRING,
    source_system STRING,
    raw_data STRING,
    load_ts TIMESTAMP,
    file_name STRING
) USING DELTA
COMMENT 'Raw shipment data from logistics systems'
""")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.inventory.raw_inventory (
    item_id STRING,
    source_system STRING,
    raw_data STRING,
    load_ts TIMESTAMP,
    file_name STRING
) USING DELTA
COMMENT 'Raw inventory data from warehouse systems'
""")

# COMMAND ----------

# Silver layer tables (conformed data)
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.supplier.suppliers (
    business_key STRING,
    natural_key STRING,
    name STRING,
    status STRING,
    region STRING,
    attributes_json STRING,
    effective_start_ts TIMESTAMP,
    effective_end_ts TIMESTAMP,
    is_current BOOLEAN,
    source_system STRING,
    load_ts TIMESTAMP
) USING DELTA
COMMENT 'Conformed supplier data with SCD2 history'
""")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.logistics.shipments (
    shipment_id STRING,
    route_id STRING,
    carrier_id STRING,
    origin STRING,
    destination STRING,
    planned_departure_ts TIMESTAMP,
    actual_departure_ts TIMESTAMP,
    planned_arrival_ts TIMESTAMP,
    actual_arrival_ts TIMESTAMP,
    status STRING,
    load_ts TIMESTAMP,
    source_system STRING
) USING DELTA
COMMENT 'Conformed shipment data'
""")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.inventory.inventory_positions (
    item_id STRING,
    location_id STRING,
    snapshot_date DATE,
    quantity_on_hand DOUBLE,
    safety_stock DOUBLE,
    in_transit_qty DOUBLE,
    load_ts TIMESTAMP,
    source_system STRING
) USING DELTA
COMMENT 'Conformed inventory position data'
""")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.conformed.crosswalks (
    source_system STRING,
    source_key STRING,
    entity_type STRING,
    business_key STRING,
    confidence DOUBLE,
    created_ts TIMESTAMP
) USING DELTA
COMMENT 'Identity resolution crosswalk mappings'
""")

# COMMAND ----------

# Gold layer tables (curated analytics)
spark.sql("""
CREATE TABLE IF NOT EXISTS gold.curated.unified_supply_chain (
    product_id STRING,
    location_id STRING,
    supplier_id STRING,
    snapshot_date DATE,
    supplier_name STRING,
    supplier_status STRING,
    shipment_id STRING,
    shipment_status STRING,
    planned_arrival TIMESTAMP,
    actual_arrival TIMESTAMP,
    quantity_on_hand DOUBLE,
    safety_stock DOUBLE,
    in_transit_qty DOUBLE,
    last_updated TIMESTAMP
) USING DELTA
COMMENT 'Unified supply chain view for analytics'
""")

# COMMAND ----------

# Operations tables
spark.sql("""
CREATE TABLE IF NOT EXISTS ops.freshness.freshness_status (
    domain STRING,
    last_updated TIMESTAMP,
    target_sla_minutes INT,
    status STRING,
    check_ts TIMESTAMP
) USING DELTA
COMMENT 'Freshness monitoring for all domains'
""")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS ops.quality.dq_status (
    dataset STRING,
    check_id STRING,
    check_result BOOLEAN,
    severity STRING,
    message STRING,
    check_ts TIMESTAMP
) USING DELTA
COMMENT 'Data quality check results'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification
# MAGIC 
# MAGIC Verify that all objects were created successfully

# COMMAND ----------

# Show all catalogs
display(spark.sql("SHOW CATALOGS"))

# COMMAND ----------

# Show schemas in each catalog
display(spark.sql("SHOW SCHEMAS IN CATALOG bronze"))
display(spark.sql("SHOW SCHEMAS IN CATALOG silver"))
display(spark.sql("SHOW SCHEMAS IN CATALOG gold"))

# COMMAND ----------

# Show tables in key schemas
display(spark.sql("SHOW TABLES IN bronze.supplier"))
display(spark.sql("SHOW TABLES IN silver.supplier"))
display(spark.sql("SHOW TABLES IN gold.curated"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete!
# MAGIC 
# MAGIC Your Unity Catalog setup is now complete. You have:
# MAGIC - 3 catalogs (bronze, silver, gold)
# MAGIC - Multiple schemas for organized data domains
# MAGIC - Role-based access control configured
# MAGIC - Core tables for the supply chain lakehouse
# MAGIC 
# MAGIC Next steps:
# MAGIC 1. Start ingesting data into the bronze layer
# MAGIC 2. Build silver layer transformations
# MAGIC 3. Create gold layer analytics views
# MAGIC 4. Set up monitoring and data quality checks
