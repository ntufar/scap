"""
Unity Catalog setup script for the supply chain lakehouse.
This script creates all necessary catalogs, schemas, and tables.
Simplified version without role permissions.
"""
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


def setup_unity_catalog() -> None:
    """Execute Unity Catalog bootstrap script."""
    logger.info("Setting up Unity Catalog objects...")
    
    # Create catalogs for each medallion layer
    spark.sql("CREATE CATALOG IF NOT EXISTS bronze")
    spark.sql("CREATE CATALOG IF NOT EXISTS silver")
    spark.sql("CREATE CATALOG IF NOT EXISTS gold")
    spark.sql("CREATE CATALOG IF NOT EXISTS ops")
    
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
    
    logger.info("Unity Catalog bootstrap completed")
    logger.info("Note: Role permissions skipped - create roles first if needed")


def create_tables() -> None:
    """Create all necessary tables."""
    logger.info("Creating tables...")
    
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
    
    # Crosswalk table for identity resolution
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
    
    logger.info("Table creation completed")


def verify_setup() -> None:
    """Verify that all objects were created successfully."""
    logger.info("Verifying setup...")
    
    # Show all catalogs
    logger.info("Created catalogs:")
    spark.sql("SHOW CATALOGS").show()
    
    # Show schemas in each catalog
    logger.info("Bronze schemas:")
    spark.sql("SHOW SCHEMAS IN bronze").show()
    
    logger.info("Silver schemas:")
    spark.sql("SHOW SCHEMAS IN silver").show()
    
    logger.info("Gold schemas:")
    spark.sql("SHOW SCHEMAS IN gold").show()
    
    # Show tables in key schemas
    logger.info("Bronze supplier tables:")
    spark.sql("SHOW TABLES IN bronze.supplier").show()
    
    logger.info("Silver supplier tables:")
    spark.sql("SHOW TABLES IN silver.supplier").show()
    
    logger.info("Gold curated tables:")
    spark.sql("SHOW TABLES IN gold.curated").show()


# Main execution
try:
    setup_unity_catalog()
    create_tables()
    verify_setup()
    logger.info("Unity Catalog setup completed successfully")
    logger.info("Next steps:")
    logger.info("1. Create roles (analysts, engineers, auditors, external_partners) if needed")
    logger.info("2. Grant appropriate permissions to roles")
    logger.info("3. Start ingesting data into bronze layer")
except Exception as e:
    logger.error(f"Unity Catalog setup failed: {e}")
    raise
