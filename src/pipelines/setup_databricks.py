"""
Databricks-specific setup for Navigator Supply Chain Lakehouse.

This script sets up Unity Catalog objects and initializes the environment
for running the supply chain data platform on Databricks.
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

logger = logging.getLogger(__name__)


def setup_databricks_environment(spark: SparkSession) -> None:
    """Setup the complete Databricks environment for the supply chain platform."""
    
    logger.info("Setting up Databricks environment for Navigator Supply Chain Lakehouse")
    
    # Create catalogs
    create_catalogs(spark)
    
    # Create schemas
    create_schemas(spark)
    
    # Create operational tables
    create_operational_tables(spark)
    
    # Create bronze tables
    create_bronze_tables(spark)
    
    # Create silver tables
    create_silver_tables(spark)
    
    # Create gold tables
    create_gold_tables(spark)
    
    logger.info("Databricks environment setup completed successfully")


def create_catalogs(spark: SparkSession) -> None:
    """Create Unity Catalog catalogs."""
    catalogs = [
        "bronze",
        "silver", 
        "gold",
        "ops"
    ]
    
    for catalog in catalogs:
        try:
            spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
            logger.info(f"Created catalog: {catalog}")
        except Exception as e:
            logger.warning(f"Could not create catalog {catalog}: {str(e)}")


def create_schemas(spark: SparkSession) -> None:
    """Create schemas within catalogs."""
    schemas = [
        ("bronze", "raw"),
        ("silver", "conformed"),
        ("gold", "curated"),
        ("ops", "monitoring")
    ]
    
    for catalog, schema in schemas:
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
            logger.info(f"Created schema: {catalog}.{schema}")
        except Exception as e:
            logger.warning(f"Could not create schema {catalog}.{schema}: {str(e)}")


def create_operational_tables(spark: SparkSession) -> None:
    """Create operational monitoring tables."""
    
    # Freshness status table
    freshness_sql = """
    CREATE TABLE IF NOT EXISTS ops.monitoring.freshness_status (
        domain STRING NOT NULL,
        last_updated TIMESTAMP NOT NULL,
        target_sla_minutes INT NOT NULL,
        status STRING NOT NULL,
        created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
    
    # Data quality status table
    dq_sql = """
    CREATE TABLE IF NOT EXISTS ops.monitoring.data_quality_status (
        table_name STRING NOT NULL,
        check_timestamp TIMESTAMP NOT NULL,
        critical_checks_passed BOOLEAN NOT NULL,
        failed_checks_count INT NOT NULL,
        quality_score DOUBLE NOT NULL,
        details STRING,
        created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
    
    # Schema validation log table
    validation_sql = """
    CREATE TABLE IF NOT EXISTS ops.monitoring.schema_validation_log (
        validation_id STRING NOT NULL,
        schema_name STRING NOT NULL,
        source_system STRING NOT NULL,
        is_valid BOOLEAN NOT NULL,
        total_records INT NOT NULL,
        valid_records INT NOT NULL,
        invalid_records INT NOT NULL,
        error_count INT NOT NULL,
        warning_count INT NOT NULL,
        validation_time TIMESTAMP NOT NULL,
        errors_json STRING,
        created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
    
    try:
        spark.sql(freshness_sql)
        spark.sql(dq_sql)
        spark.sql(validation_sql)
        logger.info("Created operational monitoring tables")
    except Exception as e:
        logger.error(f"Failed to create operational tables: {str(e)}")


def create_bronze_tables(spark: SparkSession) -> None:
    """Create bronze layer tables."""
    
    # Supplier raw table
    supplier_sql = """
    CREATE TABLE IF NOT EXISTS bronze.raw.supplier_raw (
        supplier_id STRING NOT NULL,
        supplier_name STRING NOT NULL,
        status STRING NOT NULL,
        region STRING,
        contact_email STRING,
        contact_phone STRING,
        address STRING,
        attributes STRING,
        last_updated STRING,
        _ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        _source_file STRING,
        _source_system STRING
    )
    USING DELTA
    PARTITIONED BY (_source_system)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
    
    # Shipment raw table
    shipment_sql = """
    CREATE TABLE IF NOT EXISTS bronze.raw.shipment_raw (
        shipment_id STRING NOT NULL,
        route_id STRING NOT NULL,
        carrier_id STRING NOT NULL,
        origin STRING NOT NULL,
        destination STRING NOT NULL,
        planned_departure STRING,
        actual_departure STRING,
        planned_arrival STRING,
        actual_arrival STRING,
        status STRING NOT NULL,
        tracking_number STRING,
        weight DOUBLE,
        volume DOUBLE,
        _ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        _source_file STRING,
        _source_system STRING
    )
    USING DELTA
    PARTITIONED BY (_source_system)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
    
    # Inventory raw table
    inventory_sql = """
    CREATE TABLE IF NOT EXISTS bronze.raw.inventory_raw (
        item_id STRING NOT NULL,
        location_id STRING NOT NULL,
        snapshot_date STRING NOT NULL,
        quantity_on_hand DOUBLE NOT NULL,
        safety_stock DOUBLE NOT NULL,
        in_transit_qty DOUBLE NOT NULL,
        reserved_qty DOUBLE,
        available_qty DOUBLE,
        unit_cost DOUBLE,
        last_count_date STRING,
        _ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        _source_file STRING,
        _source_system STRING
    )
    USING DELTA
    PARTITIONED BY (_source_system)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
    
    try:
        spark.sql(supplier_sql)
        spark.sql(shipment_sql)
        spark.sql(inventory_sql)
        logger.info("Created bronze layer tables")
    except Exception as e:
        logger.error(f"Failed to create bronze tables: {str(e)}")


def create_silver_tables(spark: SparkSession) -> None:
    """Create silver layer tables."""
    
    # Crosswalk table
    crosswalk_sql = """
    CREATE TABLE IF NOT EXISTS silver.conformed.crosswalk (
        source_system STRING NOT NULL,
        source_key STRING NOT NULL,
        entity_type STRING NOT NULL,
        business_key STRING NOT NULL,
        confidence DOUBLE NOT NULL DEFAULT 1.0,
        created_ts TIMESTAMP NOT NULL,
        is_active BOOLEAN NOT NULL DEFAULT true,
        updated_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
    
    # Supplier SCD2 table
    supplier_scd2_sql = """
    CREATE TABLE IF NOT EXISTS silver.conformed.supplier_scd2 (
        natural_key STRING NOT NULL,
        business_key STRING NOT NULL,
        name STRING NOT NULL,
        status STRING NOT NULL,
        region STRING,
        attributes_json STRING,
        effective_start_ts TIMESTAMP NOT NULL,
        effective_end_ts TIMESTAMP,
        is_current BOOLEAN NOT NULL,
        source_system STRING NOT NULL,
        load_ts TIMESTAMP NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    USING DELTA
    PARTITIONED BY (source_system)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
    
    # Shipment table
    shipment_sql = """
    CREATE TABLE IF NOT EXISTS silver.conformed.shipment (
        shipment_id STRING NOT NULL,
        route_id STRING NOT NULL,
        carrier_id STRING NOT NULL,
        origin STRING NOT NULL,
        destination STRING NOT NULL,
        planned_departure_ts TIMESTAMP,
        actual_departure_ts TIMESTAMP,
        planned_arrival_ts TIMESTAMP,
        actual_arrival_ts TIMESTAMP,
        status STRING NOT NULL,
        load_ts TIMESTAMP NOT NULL,
        source_system STRING NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    USING DELTA
    PARTITIONED BY (source_system, status)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
    
    # Inventory position table
    inventory_sql = """
    CREATE TABLE IF NOT EXISTS silver.conformed.inventory_position (
        item_id STRING NOT NULL,
        location_id STRING NOT NULL,
        snapshot_date DATE NOT NULL,
        quantity_on_hand DOUBLE NOT NULL,
        safety_stock DOUBLE NOT NULL,
        in_transit_qty DOUBLE NOT NULL,
        load_ts TIMESTAMP NOT NULL,
        source_system STRING NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    USING DELTA
    PARTITIONED BY (source_system, snapshot_date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
    
    try:
        spark.sql(crosswalk_sql)
        spark.sql(supplier_scd2_sql)
        spark.sql(shipment_sql)
        spark.sql(inventory_sql)
        logger.info("Created silver layer tables")
    except Exception as e:
        logger.error(f"Failed to create silver tables: {str(e)}")


def create_gold_tables(spark: SparkSession) -> None:
    """Create gold layer tables."""
    
    # Unified supply chain view (will be created as table)
    unified_sql = """
    CREATE TABLE IF NOT EXISTS gold.curated.unified_supply_chain (
        product_id STRING NOT NULL,
        location_id STRING NOT NULL,
        supplier_id STRING NOT NULL,
        supplier_name STRING,
        supplier_status STRING,
        supplier_region STRING,
        supplier_attributes STRING,
        quantity_on_hand DOUBLE,
        safety_stock DOUBLE,
        in_transit_qty DOUBLE,
        inventory_date DATE,
        shipment_id STRING,
        route_id STRING,
        carrier_id STRING,
        origin STRING,
        destination STRING,
        planned_departure_ts TIMESTAMP,
        actual_departure_ts TIMESTAMP,
        planned_arrival_ts TIMESTAMP,
        actual_arrival_ts TIMESTAMP,
        shipment_status STRING,
        stock_status STRING,
        delivery_delay_days INT,
        is_delivered BOOLEAN,
        inventory_source STRING,
        supplier_source STRING,
        shipment_source STRING,
        inventory_load_ts TIMESTAMP,
        shipment_load_ts TIMESTAMP,
        view_created_ts TIMESTAMP,
        last_data_update_ts TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    USING DELTA
    PARTITIONED BY (supplier_region, inventory_date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
    
    # Supplier performance table
    supplier_perf_sql = """
    CREATE TABLE IF NOT EXISTS gold.curated.supplier_performance (
        supplier_id STRING NOT NULL,
        supplier_name STRING,
        supplier_region STRING,
        total_shipments BIGINT,
        on_time_deliveries BIGINT,
        on_time_rate DOUBLE,
        avg_delay_days DOUBLE,
        total_products BIGINT,
        avg_inventory_turnover DOUBLE,
        performance_score DOUBLE,
        last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    USING DELTA
    PARTITIONED BY (supplier_region)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
    
    # Inventory metrics table
    inventory_metrics_sql = """
    CREATE TABLE IF NOT EXISTS gold.curated.inventory_metrics (
        product_id STRING NOT NULL,
        location_id STRING NOT NULL,
        supplier_id STRING,
        total_inventory DOUBLE,
        safety_stock_level DOUBLE,
        stockout_risk STRING,
        turnover_rate DOUBLE,
        days_in_inventory DOUBLE,
        last_movement_date DATE,
        last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    USING DELTA
    PARTITIONED BY (location_id)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
    
    # Shipment performance table
    shipment_perf_sql = """
    CREATE TABLE IF NOT EXISTS gold.curated.shipment_performance (
        carrier_id STRING NOT NULL,
        route_id STRING,
        total_shipments BIGINT,
        on_time_shipments BIGINT,
        delayed_shipments BIGINT,
        cancelled_shipments BIGINT,
        on_time_rate DOUBLE,
        avg_delay_hours DOUBLE,
        avg_transit_days DOUBLE,
        last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    USING DELTA
    PARTITIONED BY (carrier_id)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
    
    try:
        spark.sql(unified_sql)
        spark.sql(supplier_perf_sql)
        spark.sql(inventory_metrics_sql)
        spark.sql(shipment_perf_sql)
        logger.info("Created gold layer tables")
    except Exception as e:
        logger.error(f"Failed to create gold tables: {str(e)}")


def initialize_sample_data(spark: SparkSession) -> None:
    """Initialize sample data for testing."""
    
    # Sample freshness status data
    freshness_data = [
        ("supplier", current_timestamp(), 1440, "ok"),  # 24 hours
        ("logistics", current_timestamp(), 60, "ok"),   # 1 hour
        ("inventory", current_timestamp(), 5, "ok")     # 5 minutes
    ]
    
    freshness_df = spark.createDataFrame(freshness_data, ["domain", "last_updated", "target_sla_minutes", "status"])
    freshness_df.write.mode("append").saveAsTable("ops.monitoring.freshness_status")
    
    logger.info("Initialized sample operational data")


if __name__ == "__main__":
    # This can be run in a Databricks notebook
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.appName("DatabricksSetup").getOrCreate()
    
    setup_databricks_environment(spark)
    initialize_sample_data(spark)
    
    print("Databricks environment setup completed!")
