"""
Supplier SCD2 model for Navigator Supply Chain Lakehouse.

Implements Slowly Changing Dimension Type 2 (SCD2) for supplier data with
deterministic identity resolution and audit trail.
"""

from datetime import datetime
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, 
    row_number, max as spark_max, 
    coalesce, isnan, isnull
)
from pyspark.sql.window import Window


@dataclass
class SupplierRecord:
    """Supplier record with SCD2 audit fields."""
    natural_key: str
    business_key: str
    name: str
    status: str
    region: str
    attributes_json: Dict[str, Any]
    effective_start_ts: datetime
    effective_end_ts: Optional[datetime]
    is_current: bool
    source_system: str
    load_ts: datetime


class SupplierSCD2:
    """Supplier SCD2 implementation with deterministic identity resolution."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.table_name = "silver.supplier_scd2"
    
    def create_table(self) -> None:
        """Create the supplier SCD2 table with proper schema."""
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
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
        self.spark.sql(create_sql)
    
    def get_current_suppliers(self) -> DataFrame:
        """Get all current supplier records."""
        return (
            self.spark.table(self.table_name)
            .filter(col("is_current") == True)
        )
    
    def get_supplier_history(self, business_key: str) -> DataFrame:
        """Get complete history for a supplier by business key."""
        return (
            self.spark.table(self.table_name)
            .filter(col("business_key") == business_key)
            .orderBy(col("effective_start_ts"))
        )
    
    def upsert_suppliers(self, new_data: DataFrame) -> None:
        """
        Upsert supplier data with SCD2 logic.
        
        Args:
            new_data: DataFrame with columns matching SupplierRecord fields
        """
        # Ensure required columns exist
        required_cols = [
            "natural_key", "business_key", "name", "status", "region",
            "attributes_json", "source_system", "load_ts"
        ]
        
        for col_name in required_cols:
            if col_name not in new_data.columns:
                if col_name == "attributes_json":
                    new_data = new_data.withColumn(col_name, lit("{}"))
                elif col_name == "region":
                    new_data = new_data.withColumn(col_name, lit(None).cast("string"))
                else:
                    raise ValueError(f"Required column {col_name} missing from input data")
        
        # Add effective timestamps
        new_data = new_data.withColumn(
            "effective_start_ts", 
            coalesce(col("effective_start_ts"), current_timestamp())
        ).withColumn(
            "effective_end_ts", 
            lit(None).cast("timestamp")
        ).withColumn(
            "is_current", 
            lit(True)
        )
        
        # Get existing current records for comparison
        existing_current = self.get_current_suppliers()
        
        # Identify records that need updates (changed attributes)
        changed_records = self._identify_changes(new_data, existing_current)
        
        if changed_records.count() > 0:
            # Close existing records that have changes
            self._close_changed_records(changed_records)
            
            # Insert new records
            self._insert_new_records(changed_records)
    
    def _identify_changes(self, new_data: DataFrame, existing: DataFrame) -> DataFrame:
        """Identify records that have changed and need SCD2 updates."""
        # Join on business_key to find changes
        joined = new_data.alias("new").join(
            existing.alias("existing"),
            col("new.business_key") == col("existing.business_key"),
            "left"
        )
        
        # Check for changes in key attributes
        change_conditions = (
            (col("existing.business_key").isNull()) |  # New record
            (col("new.name") != col("existing.name")) |
            (col("new.status") != col("existing.status")) |
            (col("new.region") != col("existing.region")) |
            (col("new.attributes_json") != col("existing.attributes_json"))
        )
        
        return (
            joined
            .filter(change_conditions)
            .select("new.*")
        )
    
    def _close_changed_records(self, changed_records: DataFrame) -> None:
        """Close existing records that have been changed."""
        business_keys = [row.business_key for row in changed_records.select("business_key").collect()]
        
        if business_keys:
            # Update existing records to close them
            update_sql = f"""
            UPDATE {self.table_name}
            SET 
                effective_end_ts = current_timestamp(),
                is_current = false
            WHERE business_key IN ({','.join([f"'{bk}'" for bk in business_keys])})
            AND is_current = true
            """
            self.spark.sql(update_sql)
    
    def _insert_new_records(self, new_records: DataFrame) -> None:
        """Insert new SCD2 records."""
        new_records.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(self.table_name)
    
    def get_supplier_by_natural_key(self, natural_key: str, source_system: str) -> Optional[SupplierRecord]:
        """Get current supplier by natural key and source system."""
        result = (
            self.spark.table(self.table_name)
            .filter(
                (col("natural_key") == natural_key) &
                (col("source_system") == source_system) &
                (col("is_current") == True)
            )
            .collect()
        )
        
        if result:
            row = result[0]
            return SupplierRecord(
                natural_key=row.natural_key,
                business_key=row.business_key,
                name=row.name,
                status=row.status,
                region=row.region,
                attributes_json=row.attributes_json,
                effective_start_ts=row.effective_start_ts,
                effective_end_ts=row.effective_end_ts,
                is_current=row.is_current,
                source_system=row.source_system,
                load_ts=row.load_ts
            )
        return None
    
    def validate_data_quality(self) -> Dict[str, Any]:
        """Validate data quality for supplier SCD2 table."""
        df = self.spark.table(self.table_name)
        
        # Check for orphaned records (business_key without natural_key mapping)
        orphaned_count = (
            df.filter(col("business_key").isNull() | (col("business_key") == ""))
            .count()
        )
        
        # Check for duplicate current records per business_key
        duplicate_current = (
            df.filter(col("is_current") == True)
            .groupBy("business_key")
            .count()
            .filter(col("count") > 1)
            .count()
        )
        
        # Check for invalid effective date ranges
        invalid_dates = (
            df.filter(
                (col("effective_end_ts").isNotNull()) &
                (col("effective_start_ts") > col("effective_end_ts"))
            )
            .count()
        )
        
        return {
            "orphaned_records": orphaned_count,
            "duplicate_current": duplicate_current,
            "invalid_date_ranges": invalid_dates,
            "total_records": df.count(),
            "current_records": df.filter(col("is_current") == True).count()
        }


def create_supplier_scd2(spark: SparkSession) -> SupplierSCD2:
    """Factory function to create SupplierSCD2 instance."""
    return SupplierSCD2(spark)

