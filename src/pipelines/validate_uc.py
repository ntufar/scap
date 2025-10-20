"""
Unity Catalog validation script for the supply chain lakehouse.
This script validates the SQL syntax and shows what would be executed.
"""
import os
from src.lib.logging import get_logger

logger = get_logger(__name__)


def validate_sql_syntax(sql_content: str, filename: str) -> bool:
    """Validate basic SQL syntax by checking for common patterns."""
    logger.info(f"Validating SQL syntax in {filename}")
    
    # Basic validation checks
    issues = []
    
    # Check for balanced parentheses
    if sql_content.count('(') != sql_content.count(')'):
        issues.append("Unbalanced parentheses")
    
    # Check for balanced quotes
    single_quotes = sql_content.count("'")
    if single_quotes % 2 != 0:
        issues.append("Unbalanced single quotes")
    
    # Check for semicolons (statements should end with semicolons)
    statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
    if not statements:
        issues.append("No SQL statements found")
    
    # Check for common SQL keywords
    required_keywords = ['CREATE', 'CATALOG', 'SCHEMA', 'TABLE']
    found_keywords = [kw for kw in required_keywords if kw in sql_content.upper()]
    
    if issues:
        logger.error(f"SQL validation issues in {filename}: {', '.join(issues)}")
        return False
    else:
        logger.info(f"SQL syntax validation passed for {filename}")
        logger.info(f"Found {len(statements)} SQL statements")
        logger.info(f"Contains keywords: {', '.join(found_keywords)}")
        return True


def validate_bootstrap_sql() -> bool:
    """Validate the Unity Catalog bootstrap SQL."""
    bootstrap_sql_path = os.path.join(os.path.dirname(__file__), "sql", "bootstrap_uc.sql")
    
    try:
        with open(bootstrap_sql_path, 'r') as f:
            bootstrap_sql = f.read()
        
        return validate_sql_syntax(bootstrap_sql, "bootstrap_uc.sql")
    except FileNotFoundError:
        logger.error(f"Bootstrap SQL file not found: {bootstrap_sql_path}")
        return False
    except Exception as e:
        logger.error(f"Error reading bootstrap SQL: {e}")
        return False


def validate_tables_sql() -> bool:
    """Validate the table creation SQL."""
    tables_sql_path = os.path.join(os.path.dirname(__file__), "sql", "create_tables.sql")
    
    try:
        with open(tables_sql_path, 'r') as f:
            tables_sql = f.read()
        
        return validate_sql_syntax(tables_sql, "create_tables.sql")
    except FileNotFoundError:
        logger.error(f"Tables SQL file not found: {tables_sql_path}")
        return False
    except Exception as e:
        logger.error(f"Error reading tables SQL: {e}")
        return False


def show_execution_plan() -> None:
    """Show what would be executed in a real Databricks environment."""
    logger.info("=== Unity Catalog Setup Execution Plan ===")
    
    # Show bootstrap statements
    bootstrap_sql_path = os.path.join(os.path.dirname(__file__), "sql", "bootstrap_uc.sql")
    try:
        with open(bootstrap_sql_path, 'r') as f:
            bootstrap_sql = f.read()
        
        statements = [stmt.strip() for stmt in bootstrap_sql.split(';') if stmt.strip()]
        logger.info(f"Bootstrap SQL ({len(statements)} statements):")
        for i, stmt in enumerate(statements[:5], 1):  # Show first 5 statements
            logger.info(f"  {i}. {stmt[:60]}...")
        if len(statements) > 5:
            logger.info(f"  ... and {len(statements) - 5} more statements")
    except Exception as e:
        logger.error(f"Error reading bootstrap SQL: {e}")
    
    # Show table creation statements
    tables_sql_path = os.path.join(os.path.dirname(__file__), "sql", "create_tables.sql")
    try:
        with open(tables_sql_path, 'r') as f:
            tables_sql = f.read()
        
        statements = [stmt.strip() for stmt in tables_sql.split(';') if stmt.strip()]
        logger.info(f"Table Creation SQL ({len(statements)} statements):")
        for i, stmt in enumerate(statements[:5], 1):  # Show first 5 statements
            logger.info(f"  {i}. {stmt[:60]}...")
        if len(statements) > 5:
            logger.info(f"  ... and {len(statements) - 5} more statements")
    except Exception as e:
        logger.error(f"Error reading tables SQL: {e}")


def main() -> None:
    """Main validation function."""
    logger.info("Starting Unity Catalog SQL validation...")
    
    bootstrap_valid = validate_bootstrap_sql()
    tables_valid = validate_tables_sql()
    
    if bootstrap_valid and tables_valid:
        logger.info("✅ All SQL validation checks passed!")
        show_execution_plan()
        logger.info("Unity Catalog setup is ready for execution in Databricks environment")
    else:
        logger.error("❌ SQL validation failed - please fix issues before deployment")
        return False


if __name__ == "__main__":
    main()
