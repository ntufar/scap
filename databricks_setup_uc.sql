-- Unity Catalog setup script for the supply chain lakehouse
-- This script creates all necessary catalogs, schemas, and tables
-- Run this in a Databricks SQL query editor

-- =============================================
-- CREATE CATALOGS
-- =============================================

-- Create catalogs for each medallion layer
CREATE CATALOG IF NOT EXISTS bronze;
CREATE CATALOG IF NOT EXISTS silver;
CREATE CATALOG IF NOT EXISTS gold;
CREATE CATALOG IF NOT EXISTS ops;

-- =============================================
-- CREATE SCHEMAS
-- =============================================

-- Bronze layer schemas
CREATE SCHEMA IF NOT EXISTS bronze.raw;
CREATE SCHEMA IF NOT EXISTS bronze.supplier;
CREATE SCHEMA IF NOT EXISTS bronze.logistics;
CREATE SCHEMA IF NOT EXISTS bronze.inventory;

-- Silver layer schemas
CREATE SCHEMA IF NOT EXISTS silver.conformed;
CREATE SCHEMA IF NOT EXISTS silver.supplier;
CREATE SCHEMA IF NOT EXISTS silver.logistics;
CREATE SCHEMA IF NOT EXISTS silver.inventory;

-- Gold layer schemas
CREATE SCHEMA IF NOT EXISTS gold.curated;
CREATE SCHEMA IF NOT EXISTS gold.kpis;
CREATE SCHEMA IF NOT EXISTS gold.analytics;

-- Operations schemas
CREATE SCHEMA IF NOT EXISTS ops.freshness;
CREATE SCHEMA IF NOT EXISTS ops.lineage;
CREATE SCHEMA IF NOT EXISTS ops.quality;

-- =============================================
-- GRANT PERMISSIONS
-- =============================================

-- Analyst role - read access to gold layer
GRANT USE CATALOG ON CATALOG gold TO `analysts`;
GRANT USE SCHEMA ON SCHEMA gold.curated TO `analysts`;
GRANT USE SCHEMA ON SCHEMA gold.kpis TO `analysts`;
GRANT USE SCHEMA ON SCHEMA gold.analytics TO `analysts`;
GRANT SELECT ON SCHEMA gold.curated TO `analysts`;
GRANT SELECT ON SCHEMA gold.kpis TO `analysts`;
GRANT SELECT ON SCHEMA gold.analytics TO `analysts`;

-- Engineer role - full access to all layers
GRANT USE CATALOG ON CATALOG bronze TO `engineers`;
GRANT USE CATALOG ON CATALOG silver TO `engineers`;
GRANT USE CATALOG ON CATALOG gold TO `engineers`;
GRANT ALL PRIVILEGES ON CATALOG bronze TO `engineers`;
GRANT ALL PRIVILEGES ON CATALOG silver TO `engineers`;
GRANT ALL PRIVILEGES ON CATALOG gold TO `engineers`;

-- Auditor role - read access to all layers for compliance
GRANT USE CATALOG ON CATALOG bronze TO `auditors`;
GRANT USE CATALOG ON CATALOG silver TO `auditors`;
GRANT USE CATALOG ON CATALOG gold TO `auditors`;
GRANT SELECT ON CATALOG bronze TO `auditors`;
GRANT SELECT ON CATALOG silver TO `auditors`;
GRANT SELECT ON CATALOG gold TO `auditors`;

-- External partner role - limited access to specific gold views
GRANT USE CATALOG ON CATALOG gold TO `external_partners`;
GRANT USE SCHEMA ON SCHEMA gold.curated TO `external_partners`;
GRANT SELECT ON SCHEMA gold.curated TO `external_partners`;

-- =============================================
-- CREATE TABLES
-- =============================================

-- Bronze layer tables (raw data)
CREATE TABLE IF NOT EXISTS bronze.supplier.raw_suppliers (
    natural_key STRING,
    source_system STRING,
    raw_data STRING,
    load_ts TIMESTAMP,
    file_name STRING
) USING DELTA
COMMENT 'Raw supplier data from source systems';

CREATE TABLE IF NOT EXISTS bronze.logistics.raw_shipments (
    shipment_id STRING,
    source_system STRING,
    raw_data STRING,
    load_ts TIMESTAMP,
    file_name STRING
) USING DELTA
COMMENT 'Raw shipment data from logistics systems';

CREATE TABLE IF NOT EXISTS bronze.inventory.raw_inventory (
    item_id STRING,
    source_system STRING,
    raw_data STRING,
    load_ts TIMESTAMP,
    file_name STRING
) USING DELTA
COMMENT 'Raw inventory data from warehouse systems';

-- Silver layer tables (conformed data)
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
COMMENT 'Conformed supplier data with SCD2 history';

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
COMMENT 'Conformed shipment data';

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
COMMENT 'Conformed inventory position data';

-- Crosswalk table for identity resolution
CREATE TABLE IF NOT EXISTS silver.conformed.crosswalks (
    source_system STRING,
    source_key STRING,
    entity_type STRING,
    business_key STRING,
    confidence DOUBLE,
    created_ts TIMESTAMP
) USING DELTA
COMMENT 'Identity resolution crosswalk mappings';

-- Gold layer tables (curated analytics)
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
COMMENT 'Unified supply chain view for analytics';

-- Operations tables
CREATE TABLE IF NOT EXISTS ops.freshness.freshness_status (
    domain STRING,
    last_updated TIMESTAMP,
    target_sla_minutes INT,
    status STRING,
    check_ts TIMESTAMP
) USING DELTA
COMMENT 'Freshness monitoring for all domains';

CREATE TABLE IF NOT EXISTS ops.quality.dq_status (
    dataset STRING,
    check_id STRING,
    check_result BOOLEAN,
    severity STRING,
    message STRING,
    check_ts TIMESTAMP
) USING DELTA
COMMENT 'Data quality check results';

-- =============================================
-- VERIFICATION QUERIES
-- =============================================

-- Show all catalogs
SHOW CATALOGS;

-- Show schemas in each catalog
SHOW SCHEMAS IN CATALOG bronze;
SHOW SCHEMAS IN CATALOG silver;
SHOW SCHEMAS IN CATALOG gold;

-- Show tables in key schemas
SHOW TABLES IN bronze.supplier;
SHOW TABLES IN silver.supplier;
SHOW TABLES IN gold.curated;
