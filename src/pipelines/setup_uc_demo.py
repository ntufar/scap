"""
Demo script for Unity Catalog setup.
This script shows the SQL statements that would be executed for the supply chain lakehouse.
"""
import os
from src.lib.logging import get_logger

logger = get_logger(__name__)


def show_bootstrap_sql() -> None:
    """Display the Unity Catalog bootstrap SQL statements."""
    logger.info("=== Unity Catalog Bootstrap SQL ===")
    
    bootstrap_sql_path = os.path.join(os.path.dirname(__file__), "sql", "bootstrap_uc.sql")
    with open(bootstrap_sql_path, 'r') as f:
        bootstrap_sql = f.read()
    
    print("\n" + "="*60)
    print("UNITY CATALOG BOOTSTRAP SQL")
    print("="*60)
    print(bootstrap_sql)
    print("="*60)


def show_tables_sql() -> None:
    """Display the table creation SQL statements."""
    logger.info("=== Table Creation SQL ===")
    
    tables_sql_path = os.path.join(os.path.dirname(__file__), "sql", "create_tables.sql")
    with open(tables_sql_path, 'r') as f:
        tables_sql = f.read()
    
    print("\n" + "="*60)
    print("TABLE CREATION SQL")
    print("="*60)
    print(tables_sql)
    print("="*60)


def show_local_equivalent() -> None:
    """Show the local Spark equivalent SQL statements."""
    logger.info("=== Local Spark Equivalent ===")
    
    print("\n" + "="*60)
    print("LOCAL SPARK EQUIVALENT SQL")
    print("="*60)
    print("""
-- For local development, use databases instead of catalogs
-- and schemas instead of nested schemas

-- Create databases (equivalent to catalogs)
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;
CREATE DATABASE IF NOT EXISTS ops;

-- Create schemas within each database
-- Bronze layer
CREATE SCHEMA IF NOT EXISTS bronze.raw;
CREATE SCHEMA IF NOT EXISTS bronze.supplier;
CREATE SCHEMA IF NOT EXISTS bronze.logistics;
CREATE SCHEMA IF NOT EXISTS bronze.inventory;

-- Silver layer
CREATE SCHEMA IF NOT EXISTS silver.conformed;
CREATE SCHEMA IF NOT EXISTS silver.supplier;
CREATE SCHEMA IF NOT EXISTS silver.logistics;
CREATE SCHEMA IF NOT EXISTS silver.inventory;

-- Gold layer
CREATE SCHEMA IF NOT EXISTS gold.curated;
CREATE SCHEMA IF NOT EXISTS gold.kpis;
CREATE SCHEMA IF NOT EXISTS gold.analytics;

-- Operations
CREATE SCHEMA IF NOT EXISTS ops.freshness;
CREATE SCHEMA IF NOT EXISTS ops.lineage;
CREATE SCHEMA IF NOT EXISTS ops.quality;

-- Note: GRANT statements are not supported in local Spark
-- They would need to be handled by your security layer
""")
    print("="*60)


def main() -> None:
    """Main demo function."""
    logger.info("Supply Chain Analytics Platform - SQL Demo")
    
    try:
        show_bootstrap_sql()
        show_tables_sql()
        show_local_equivalent()
        
        logger.info("Demo completed successfully")
        print("\n" + "="*60)
        print("NEXT STEPS:")
        print("="*60)
        print("1. For Databricks: Run the original setup_uc.py script")
        print("2. For local development: Use the local equivalent SQL")
        print("3. For production: Deploy using Databricks CLI or dbx")
        print("="*60)
        
    except Exception as e:
        logger.error(f"Demo failed: {e}")
        raise


if __name__ == "__main__":
    main()
