"""
Crosswalk model for identity resolution in the Navigator Supply Chain Lakehouse.

This module provides deterministic identity resolution across different source systems
by maintaining crosswalks between source keys and canonical business keys.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dataclass
class CrosswalkRecord:
    """Represents a single crosswalk mapping between source and business keys."""
    
    source_system: str
    source_key: str
    entity_type: str  # 'supplier', 'product', 'location', 'shipment', etc.
    business_key: str
    confidence: float = 1.0  # Default to deterministic (1.0)
    created_ts: datetime = None
    is_active: bool = True
    
    def __post_init__(self):
        if self.created_ts is None:
            self.created_ts = datetime.utcnow()


class CrosswalkManager:
    """Manages crosswalk operations for identity resolution."""
    
    def __init__(self, spark_session=None):
        self.spark = spark_session
        self.crosswalk_table = "bronze.crosswalk"
    
    def create_crosswalk_table(self) -> None:
        """Create the crosswalk table in Unity Catalog if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.crosswalk_table} (
            source_system STRING NOT NULL,
            source_key STRING NOT NULL,
            entity_type STRING NOT NULL,
            business_key STRING NOT NULL,
            confidence DOUBLE NOT NULL DEFAULT 1.0,
            created_ts TIMESTAMP NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT true,
            updated_ts TIMESTAMP NOT NULL DEFAULT current_timestamp()
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """
        
        if self.spark:
            self.spark.sql(create_table_sql)
            logger.info(f"Created crosswalk table: {self.crosswalk_table}")
        else:
            logger.warning("No Spark session available for table creation")
    
    def add_crosswalk_mapping(
        self, 
        source_system: str, 
        source_key: str, 
        entity_type: str, 
        business_key: str,
        confidence: float = 1.0
    ) -> None:
        """Add a new crosswalk mapping."""
        record = CrosswalkRecord(
            source_system=source_system,
            source_key=source_key,
            entity_type=entity_type,
            business_key=business_key,
            confidence=confidence
        )
        
        if self.spark:
            # Convert to DataFrame and insert
            from pyspark.sql import Row
            from pyspark.sql.functions import current_timestamp
            
            row = Row(
                source_system=record.source_system,
                source_key=record.source_key,
                entity_type=record.entity_type,
                business_key=record.business_key,
                confidence=record.confidence,
                created_ts=record.created_ts,
                is_active=record.is_active,
                updated_ts=current_timestamp()
            )
            
            df = self.spark.createDataFrame([row])
            df.write.mode("append").saveAsTable(self.crosswalk_table)
            logger.info(f"Added crosswalk mapping: {source_system}:{source_key} -> {business_key}")
        else:
            logger.warning("No Spark session available for crosswalk addition")
    
    def resolve_business_key(
        self, 
        source_system: str, 
        source_key: str, 
        entity_type: str
    ) -> Optional[str]:
        """Resolve a source key to its canonical business key."""
        if not self.spark:
            logger.warning("No Spark session available for key resolution")
            return None
        
        query = f"""
        SELECT business_key, confidence
        FROM {self.crosswalk_table}
        WHERE source_system = '{source_system}'
          AND source_key = '{source_key}'
          AND entity_type = '{entity_type}'
          AND is_active = true
        ORDER BY confidence DESC, created_ts DESC
        LIMIT 1
        """
        
        result = self.spark.sql(query).collect()
        if result:
            business_key = result[0]['business_key']
            confidence = result[0]['confidence']
            logger.debug(f"Resolved {source_system}:{source_key} -> {business_key} (confidence: {confidence})")
            return business_key
        else:
            logger.warning(f"No business key found for {source_system}:{source_key} ({entity_type})")
            return None
    
    def get_all_mappings_for_entity(self, entity_type: str) -> List[Dict]:
        """Get all crosswalk mappings for a specific entity type."""
        if not self.spark:
            logger.warning("No Spark session available for mapping retrieval")
            return []
        
        query = f"""
        SELECT source_system, source_key, business_key, confidence, created_ts
        FROM {self.crosswalk_table}
        WHERE entity_type = '{entity_type}'
          AND is_active = true
        ORDER BY source_system, source_key
        """
        
        result = self.spark.sql(query).collect()
        return [row.asDict() for row in result]
    
    def deactivate_mapping(
        self, 
        source_system: str, 
        source_key: str, 
        entity_type: str
    ) -> None:
        """Deactivate a crosswalk mapping (soft delete)."""
        if not self.spark:
            logger.warning("No Spark session available for mapping deactivation")
            return
        
        update_sql = f"""
        UPDATE {self.crosswalk_table}
        SET is_active = false, updated_ts = current_timestamp()
        WHERE source_system = '{source_system}'
          AND source_key = '{source_key}'
          AND entity_type = '{entity_type}'
          AND is_active = true
        """
        
        self.spark.sql(update_sql)
        logger.info(f"Deactivated crosswalk mapping: {source_system}:{source_key} ({entity_type})")
    
    def validate_crosswalk_integrity(self) -> Dict[str, int]:
        """Validate crosswalk data integrity and return statistics."""
        if not self.spark:
            logger.warning("No Spark session available for integrity validation")
            return {}
        
        # Check for duplicate active mappings
        duplicate_check = f"""
        SELECT source_system, source_key, entity_type, COUNT(*) as count
        FROM {self.crosswalk_table}
        WHERE is_active = true
        GROUP BY source_system, source_key, entity_type
        HAVING COUNT(*) > 1
        """
        
        duplicates = self.spark.sql(duplicate_check).collect()
        
        # Get total active mappings
        total_check = f"""
        SELECT COUNT(*) as total_active
        FROM {self.crosswalk_table}
        WHERE is_active = true
        """
        
        total_result = self.spark.sql(total_check).collect()
        total_active = total_result[0]['total_active'] if total_result else 0
        
        return {
            'total_active_mappings': total_active,
            'duplicate_mappings': len(duplicates),
            'duplicate_details': [row.asDict() for row in duplicates]
        }


def create_default_crosswalks(spark_session, domain: str = "supplier") -> None:
    """Create default crosswalk mappings for common entities."""
    manager = CrosswalkManager(spark_session)
    manager.create_crosswalk_table()
    
    # Example default mappings for supplier domain
    if domain == "supplier":
        # Sample supplier crosswalks
        sample_mappings = [
            ("erp_system", "SUP001", "supplier", "SUP_ACME_CORP", 1.0),
            ("erp_system", "SUP002", "supplier", "SUP_BETA_INC", 1.0),
            ("wms_system", "VENDOR_123", "supplier", "SUP_ACME_CORP", 1.0),
            ("wms_system", "VENDOR_456", "supplier", "SUP_BETA_INC", 1.0),
        ]
        
        for source_system, source_key, entity_type, business_key, confidence in sample_mappings:
            manager.add_crosswalk_mapping(
                source_system, source_key, entity_type, business_key, confidence
            )
        
        logger.info(f"Created {len(sample_mappings)} default crosswalk mappings for {domain} domain")
