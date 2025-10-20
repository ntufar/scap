"""
Bronze ingestion jobs for Navigator Supply Chain Lakehouse.

Handles raw data ingestion from various sources into bronze layer tables.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_timestamp, 
    from_json, regexp_replace, trim, upper
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

from src.lib.logging import get_logger
from src.lib.schema_validation import validate_schema, SchemaValidationError


logger = get_logger(__name__)


class BronzeIngestionJob:
    """Base class for bronze ingestion jobs."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = get_logger(self.__class__.__name__)
    
    def create_bronze_table(self, table_name: str, schema: StructType, 
                          partition_cols: List[str] = None) -> None:
        """Create a bronze table with the specified schema."""
        partition_clause = ""
        if partition_cols:
            partition_clause = f"PARTITIONED BY ({', '.join(partition_cols)})"
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {self._schema_to_ddl(schema)},
            _ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            _source_file STRING,
            _source_system STRING
        )
        USING DELTA
        {partition_clause}
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """
        self.spark.sql(create_sql)
        self.logger.info(f"Created bronze table: {table_name}")
    
    def _schema_to_ddl(self, schema: StructType) -> str:
        """Convert Spark schema to DDL string."""
        fields = []
        for field in schema.fields:
            field_type = field.dataType.simpleString().upper()
            nullable = "NULL" if field.nullable else "NOT NULL"
            fields.append(f"{field.name} {field_type} {nullable}")
        return ",\n            ".join(fields)
    
    def ingest_raw_data(self, source_path: str, table_name: str, 
                       schema: StructType, source_system: str) -> None:
        """Ingest raw data from source path to bronze table."""
        try:
            self.logger.info(f"Starting ingestion from {source_path} to {table_name}")
            
            # Read raw data
            raw_df = self._read_source_data(source_path, schema)
            
            # Add metadata columns
            enriched_df = raw_df.withColumn("_ingestion_ts", current_timestamp()) \
                              .withColumn("_source_file", lit(source_path)) \
                              .withColumn("_source_system", lit(source_system))
            
            # Write to bronze table
            enriched_df.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(table_name)
            
            self.logger.info(f"Successfully ingested {raw_df.count()} records to {table_name}")
            
        except Exception as e:
            self.logger.error(f"Failed to ingest data from {source_path}: {str(e)}")
            raise


class SupplierBronzeJob(BronzeIngestionJob):
    """Bronze ingestion job for supplier data."""
    
    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self.table_name = "bronze.supplier_raw"
        self.source_system = "supplier_system"
    
    def get_schema(self) -> StructType:
        """Get schema for supplier raw data."""
        return StructType([
            StructField("supplier_id", StringType(), False),
            StructField("supplier_name", StringType(), False),
            StructField("status", StringType(), False),
            StructField("region", StringType(), True),
            StructField("contact_email", StringType(), True),
            StructField("contact_phone", StringType(), True),
            StructField("address", StringType(), True),
            StructField("attributes", StringType(), True),  # JSON string
            StructField("last_updated", StringType(), True)
        ])
    
    def create_table(self) -> None:
        """Create bronze supplier table."""
        schema = self.get_schema()
        self.create_bronze_table(
            self.table_name, 
            schema, 
            partition_cols=["_source_system"]
        )
    
    def ingest_supplier_data(self, source_path: str) -> None:
        """Ingest supplier data from source."""
        schema = self.get_schema()
        self.ingest_raw_data(source_path, self.table_name, schema, self.source_system)
    
    def _read_source_data(self, source_path: str, schema: StructType) -> DataFrame:
        """Read supplier data from various source formats."""
        if source_path.endswith('.json'):
            return self._read_json_data(source_path, schema)
        elif source_path.endswith('.csv'):
            return self._read_csv_data(source_path, schema)
        else:
            raise ValueError(f"Unsupported file format: {source_path}")
    
    def _read_json_data(self, source_path: str, schema: StructType) -> DataFrame:
        """Read JSON data with proper parsing."""
        df = self.spark.read.json(source_path)
        
        # Parse JSON attributes field if present
        if "attributes" in df.columns:
            df = df.withColumn(
                "attributes",
                when(col("attributes").isNotNull(), col("attributes"))
                .otherwise(lit("{}"))
            )
        
        # Parse timestamp fields
        if "last_updated" in df.columns:
            df = df.withColumn(
                "last_updated",
                to_timestamp(col("last_updated"), "yyyy-MM-dd HH:mm:ss")
            )
        
        return df
    
    def _read_csv_data(self, source_path: str, schema: StructType) -> DataFrame:
        """Read CSV data with proper parsing."""
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .csv(source_path)
        
        # Clean and standardize string fields
        for field in schema.fields:
            if field.dataType == StringType():
                df = df.withColumn(
                    field.name,
                    trim(upper(col(field.name)))
                )
        
        return df


class ShipmentBronzeJob(BronzeIngestionJob):
    """Bronze ingestion job for shipment data."""
    
    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self.table_name = "bronze.shipment_raw"
        self.source_system = "logistics_system"
    
    def get_schema(self) -> StructType:
        """Get schema for shipment raw data."""
        return StructType([
            StructField("shipment_id", StringType(), False),
            StructField("route_id", StringType(), False),
            StructField("carrier_id", StringType(), False),
            StructField("origin", StringType(), False),
            StructField("destination", StringType(), False),
            StructField("planned_departure", StringType(), True),
            StructField("actual_departure", StringType(), True),
            StructField("planned_arrival", StringType(), True),
            StructField("actual_arrival", StringType(), True),
            StructField("status", StringType(), False),
            StructField("tracking_number", StringType(), True),
            StructField("weight", DoubleType(), True),
            StructField("volume", DoubleType(), True)
        ])
    
    def create_table(self) -> None:
        """Create bronze shipment table."""
        schema = self.get_schema()
        self.create_bronze_table(
            self.table_name, 
            schema, 
            partition_cols=["_source_system"]
        )
    
    def ingest_shipment_data(self, source_path: str) -> None:
        """Ingest shipment data from source."""
        schema = self.get_schema()
        self.ingest_raw_data(source_path, self.table_name, schema, self.source_system)
    
    def _read_source_data(self, source_path: str, schema: StructType) -> DataFrame:
        """Read shipment data from various source formats."""
        if source_path.endswith('.json'):
            return self._read_json_data(source_path, schema)
        elif source_path.endswith('.csv'):
            return self._read_csv_data(source_path, schema)
        else:
            raise ValueError(f"Unsupported file format: {source_path}")
    
    def _read_json_data(self, source_path: str, schema: StructType) -> DataFrame:
        """Read JSON data with proper parsing."""
        df = self.spark.read.json(source_path)
        
        # Parse timestamp fields
        timestamp_fields = ["planned_departure", "actual_departure", "planned_arrival", "actual_arrival"]
        for field in timestamp_fields:
            if field in df.columns:
                df = df.withColumn(
                    field,
                    to_timestamp(col(field), "yyyy-MM-dd HH:mm:ss")
                )
        
        return df
    
    def _read_csv_data(self, source_path: str, schema: StructType) -> DataFrame:
        """Read CSV data with proper parsing."""
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .csv(source_path)
        
        # Parse timestamp fields
        timestamp_fields = ["planned_departure", "actual_departure", "planned_arrival", "actual_arrival"]
        for field in timestamp_fields:
            if field in df.columns:
                df = df.withColumn(
                    field,
                    to_timestamp(col(field), "yyyy-MM-dd HH:mm:ss")
                )
        
        return df


class InventoryBronzeJob(BronzeIngestionJob):
    """Bronze ingestion job for inventory data."""
    
    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self.table_name = "bronze.inventory_raw"
        self.source_system = "inventory_system"
    
    def get_schema(self) -> StructType:
        """Get schema for inventory raw data."""
        return StructType([
            StructField("item_id", StringType(), False),
            StructField("location_id", StringType(), False),
            StructField("snapshot_date", StringType(), False),
            StructField("quantity_on_hand", DoubleType(), False),
            StructField("safety_stock", DoubleType(), False),
            StructField("in_transit_qty", DoubleType(), False),
            StructField("reserved_qty", DoubleType(), True),
            StructField("available_qty", DoubleType(), True),
            StructField("unit_cost", DoubleType(), True),
            StructField("last_count_date", StringType(), True)
        ])
    
    def create_table(self) -> None:
        """Create bronze inventory table."""
        schema = self.get_schema()
        self.create_bronze_table(
            self.table_name, 
            schema, 
            partition_cols=["_source_system"]
        )
    
    def ingest_inventory_data(self, source_path: str) -> None:
        """Ingest inventory data from source."""
        schema = self.get_schema()
        self.ingest_raw_data(source_path, self.table_name, schema, self.source_system)
    
    def _read_source_data(self, source_path: str, schema: StructType) -> DataFrame:
        """Read inventory data from various source formats."""
        if source_path.endswith('.json'):
            return self._read_json_data(source_path, schema)
        elif source_path.endswith('.csv'):
            return self._read_csv_data(source_path, schema)
        else:
            raise ValueError(f"Unsupported file format: {source_path}")
    
    def _read_json_data(self, source_path: str, schema: StructType) -> DataFrame:
        """Read JSON data with proper parsing."""
        df = self.spark.read.json(source_path)
        
        # Parse date fields
        if "snapshot_date" in df.columns:
            df = df.withColumn(
                "snapshot_date",
                to_timestamp(col("snapshot_date"), "yyyy-MM-dd").cast("date")
            )
        
        if "last_count_date" in df.columns:
            df = df.withColumn(
                "last_count_date",
                to_timestamp(col("last_count_date"), "yyyy-MM-dd").cast("date")
            )
        
        return df
    
    def _read_csv_data(self, source_path: str, schema: StructType) -> DataFrame:
        """Read CSV data with proper parsing."""
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .csv(source_path)
        
        # Parse date fields
        if "snapshot_date" in df.columns:
            df = df.withColumn(
                "snapshot_date",
                to_timestamp(col("snapshot_date"), "yyyy-MM-dd").cast("date")
            )
        
        if "last_count_date" in df.columns:
            df = df.withColumn(
                "last_count_date",
                to_timestamp(col("last_count_date"), "yyyy-MM-dd").cast("date")
            )
        
        return df


def create_bronze_jobs(spark: SparkSession) -> Dict[str, BronzeIngestionJob]:
    """Factory function to create all bronze ingestion jobs."""
    return {
        "supplier": SupplierBronzeJob(spark),
        "shipment": ShipmentBronzeJob(spark),
        "inventory": InventoryBronzeJob(spark)
    }


def run_bronze_ingestion(spark: SparkSession, domain: str, source_path: str) -> None:
    """Run bronze ingestion for a specific domain."""
    jobs = create_bronze_jobs(spark)
    
    if domain not in jobs:
        raise ValueError(f"Unknown domain: {domain}. Available: {list(jobs.keys())}")
    
    job = jobs[domain]
    job.create_table()
    
    if domain == "supplier":
        job.ingest_supplier_data(source_path)
    elif domain == "shipment":
        job.ingest_shipment_data(source_path)
    elif domain == "inventory":
        job.ingest_inventory_data(source_path)
    
    logger.info(f"Completed bronze ingestion for domain: {domain}")


if __name__ == "__main__":
    # Example usage
    spark = SparkSession.builder \
        .appName("BronzeIngestion") \
        .getOrCreate()
    
    # Create all bronze tables
    jobs = create_bronze_jobs(spark)
    for job in jobs.values():
        job.create_table()
    
    spark.stop()