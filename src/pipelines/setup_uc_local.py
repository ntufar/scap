"""
Local Spark setup script for the supply chain lakehouse.
This script creates databases and tables for local development.
"""
import os
from pyspark.sql import SparkSession
from src.lib.logging import get_logger

logger = get_logger(__name__)


def create_spark_session() -> SparkSession:
    """Create Spark session for local development."""
    return SparkSession.builder \
        .appName("scap-local-setup") \
        .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .getOrCreate()


def setup_local_databases(spark: SparkSession) -> None:
    """Create local databases for bronze/silver/gold architecture."""
    logger.info("Setting up local databases...")
    
    # Create databases (equivalent to catalogs in Unity Catalog)
    databases = ["bronze", "silver", "gold", "ops"]
    
    for db in databases:
        logger.info(f"Creating database: {db}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    
    logger.info("Local databases setup completed")


def create_schemas(spark: SparkSession) -> None:
    """Create schemas within each database."""
    logger.info("Creating schemas...")
    
    # Bronze layer schemas
    bronze_schemas = ["raw", "supplier", "logistics", "inventory"]
    for schema in bronze_schemas:
        logger.info(f"Creating schema: bronze.{schema}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze.{schema}")
    
    # Silver layer schemas
    silver_schemas = ["conformed", "supplier", "logistics", "inventory"]
    for schema in silver_schemas:
        logger.info(f"Creating schema: silver.{schema}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver.{schema}")
    
    # Gold layer schemas
    gold_schemas = ["curated", "kpis", "analytics"]
    for schema in gold_schemas:
        logger.info(f"Creating schema: gold.{schema}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS gold.{schema}")
    
    # Operations schemas
    ops_schemas = ["freshness", "lineage", "quality"]
    for schema in ops_schemas:
        logger.info(f"Creating schema: ops.{schema}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS ops.{schema}")
    
    logger.info("Schema creation completed")


def create_tables(spark: SparkSession) -> None:
    """Create all necessary tables."""
    logger.info("Creating tables...")
    
    # Read and execute table creation SQL
    tables_sql_path = os.path.join(os.path.dirname(__file__), "sql", "create_tables.sql")
    with open(tables_sql_path, 'r') as f:
        tables_sql = f.read()
    
    # Split by semicolon and execute each statement
    statements = [stmt.strip() for stmt in tables_sql.split(';') if stmt.strip()]
    for statement in statements:
        if statement and not statement.startswith('--'):
            logger.info(f"Executing: {statement[:50]}...")
            try:
                spark.sql(statement)
            except Exception as e:
                logger.warning(f"Failed to execute statement: {statement[:50]}... Error: {e}")
    
    logger.info("Table creation completed")


def main() -> None:
    """Main setup function for local development."""
    spark = create_spark_session()
    
    try:
        setup_local_databases(spark)
        create_schemas(spark)
        create_tables(spark)
        logger.info("Local setup completed successfully")
        
        # Show what was created
        logger.info("Created databases:")
        spark.sql("SHOW DATABASES").show()
        
    except Exception as e:
        logger.error(f"Local setup failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
