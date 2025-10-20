"""
Unity Catalog setup script for the supply chain lakehouse.
This script creates all necessary catalogs, schemas, and tables.
"""
import os
from pyspark.sql import SparkSession
from src.lib.logging import get_logger

logger = get_logger(__name__)


def create_spark_session() -> SparkSession:
    """Create Spark session with Unity Catalog configuration."""
    return SparkSession.builder \
        .appName("scap-uc-setup") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .getOrCreate()


def setup_unity_catalog(spark: SparkSession) -> None:
    """Execute Unity Catalog bootstrap script."""
    logger.info("Setting up Unity Catalog objects...")
    
    # Read and execute bootstrap SQL
    bootstrap_sql_path = os.path.join(os.path.dirname(__file__), "sql", "bootstrap_uc.sql")
    with open(bootstrap_sql_path, 'r') as f:
        bootstrap_sql = f.read()
    
    # Split by semicolon and execute each statement
    statements = [stmt.strip() for stmt in bootstrap_sql.split(';') if stmt.strip()]
    for statement in statements:
        if statement:
            logger.info(f"Executing: {statement[:50]}...")
            spark.sql(statement)
    
    logger.info("Unity Catalog bootstrap completed")


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
        if statement:
            logger.info(f"Executing: {statement[:50]}...")
            spark.sql(statement)
    
    logger.info("Table creation completed")


def main() -> None:
    """Main setup function."""
    spark = create_spark_session()
    
    try:
        setup_unity_catalog(spark)
        create_tables(spark)
        logger.info("Unity Catalog setup completed successfully")
    except Exception as e:
        logger.error(f"Unity Catalog setup failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
