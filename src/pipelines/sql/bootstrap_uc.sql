-- Unity Catalog bootstrap: create catalogs and schemas for bronze/silver/gold
-- This script sets up the medallion architecture for the supply chain lakehouse

-- Create catalogs for each medallion layer
CREATE CATALOG IF NOT EXISTS bronze;
CREATE CATALOG IF NOT EXISTS silver;
CREATE CATALOG IF NOT EXISTS gold;

-- Create schemas for organized data domains
CREATE SCHEMA IF NOT EXISTS bronze.raw;
CREATE SCHEMA IF NOT EXISTS bronze.supplier;
CREATE SCHEMA IF NOT EXISTS bronze.logistics;
CREATE SCHEMA IF NOT EXISTS bronze.inventory;

CREATE SCHEMA IF NOT EXISTS silver.conformed;
CREATE SCHEMA IF NOT EXISTS silver.supplier;
CREATE SCHEMA IF NOT EXISTS silver.logistics;
CREATE SCHEMA IF NOT EXISTS silver.inventory;

CREATE SCHEMA IF NOT EXISTS gold.curated;
CREATE SCHEMA IF NOT EXISTS gold.kpis;
CREATE SCHEMA IF NOT EXISTS gold.analytics;

-- Create operations schema for metadata and monitoring
CREATE SCHEMA IF NOT EXISTS ops.freshness;
CREATE SCHEMA IF NOT EXISTS ops.lineage;
CREATE SCHEMA IF NOT EXISTS ops.quality;

-- Grant permissions for different roles
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


