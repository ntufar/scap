"""
Unity Catalog role and permission setup script.
Run this after the basic Unity Catalog setup is complete.
"""
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


def create_roles() -> None:
    """Create the required roles for the supply chain lakehouse."""
    logger.info("Creating roles...")
    
    # Create roles
    spark.sql("CREATE ROLE IF NOT EXISTS analysts")
    spark.sql("CREATE ROLE IF NOT EXISTS engineers") 
    spark.sql("CREATE ROLE IF NOT EXISTS auditors")
    spark.sql("CREATE ROLE IF NOT EXISTS external_partners")
    
    logger.info("Roles created successfully")


def grant_permissions() -> None:
    """Grant permissions to roles."""
    logger.info("Granting permissions...")
    
    # Analyst role - read access to gold layer
    spark.sql("GRANT USE CATALOG ON CATALOG gold TO `analysts`")
    spark.sql("GRANT USE SCHEMA ON SCHEMA gold.curated TO `analysts`")
    spark.sql("GRANT USE SCHEMA ON SCHEMA gold.kpis TO `analysts`")
    spark.sql("GRANT USE SCHEMA ON SCHEMA gold.analytics TO `analysts`")
    spark.sql("GRANT SELECT ON SCHEMA gold.curated TO `analysts`")
    spark.sql("GRANT SELECT ON SCHEMA gold.kpis TO `analysts`")
    spark.sql("GRANT SELECT ON SCHEMA gold.analytics TO `analysts`")
    
    # Engineer role - full access to all layers
    spark.sql("GRANT USE CATALOG ON CATALOG bronze TO `engineers`")
    spark.sql("GRANT USE CATALOG ON CATALOG silver TO `engineers`")
    spark.sql("GRANT USE CATALOG ON CATALOG gold TO `engineers`")
    spark.sql("GRANT ALL PRIVILEGES ON CATALOG bronze TO `engineers`")
    spark.sql("GRANT ALL PRIVILEGES ON CATALOG silver TO `engineers`")
    spark.sql("GRANT ALL PRIVILEGES ON CATALOG gold TO `engineers`")
    
    # Auditor role - read access to all layers for compliance
    spark.sql("GRANT USE CATALOG ON CATALOG bronze TO `auditors`")
    spark.sql("GRANT USE CATALOG ON CATALOG silver TO `auditors`")
    spark.sql("GRANT USE CATALOG ON CATALOG gold TO `auditors`")
    spark.sql("GRANT SELECT ON CATALOG bronze TO `auditors`")
    spark.sql("GRANT SELECT ON CATALOG silver TO `auditors`")
    spark.sql("GRANT SELECT ON CATALOG gold TO `auditors`")
    
    # External partner role - limited access to specific gold views
    spark.sql("GRANT USE CATALOG ON CATALOG gold TO `external_partners`")
    spark.sql("GRANT USE SCHEMA ON SCHEMA gold.curated TO `external_partners`")
    spark.sql("GRANT SELECT ON SCHEMA gold.curated TO `external_partners`")
    
    logger.info("Permissions granted successfully")


def verify_roles() -> None:
    """Verify that roles were created and permissions granted."""
    logger.info("Verifying roles...")
    
    # Show roles
    spark.sql("SHOW ROLES").show()
    
    # Show permissions for each role
    logger.info("Analysts permissions:")
    spark.sql("SHOW GRANT ON ROLE analysts").show()
    
    logger.info("Engineers permissions:")
    spark.sql("SHOW GRANT ON ROLE engineers").show()
    
    logger.info("Auditors permissions:")
    spark.sql("SHOW GRANT ON ROLE auditors").show()
    
    logger.info("External partners permissions:")
    spark.sql("SHOW GRANT ON ROLE external_partners").show()


# Main execution
try:
    create_roles()
    grant_permissions()
    verify_roles()
    logger.info("Role and permission setup completed successfully")
except Exception as e:
    logger.error(f"Role setup failed: {e}")
    raise
