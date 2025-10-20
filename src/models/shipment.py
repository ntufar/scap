"""
Shipment model for Navigator Supply Chain Lakehouse.

Handles shipment tracking data with route, carrier, and timing information.
"""

from datetime import datetime
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, 
    coalesce, isnan, isnull, to_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType


@dataclass
class ShipmentRecord:
    """Shipment record with audit fields."""
    shipment_id: str
    route_id: str
    carrier_id: str
    origin: str
    destination: str
    planned_departure_ts: Optional[datetime]
    actual_departure_ts: Optional[datetime]
    planned_arrival_ts: Optional[datetime]
    actual_arrival_ts: Optional[datetime]
    status: str
    load_ts: datetime
    source_system: str


class ShipmentModel:
    """Shipment model implementation."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.table_name = "silver.shipment"
    
    def create_table(self) -> None:
        """Create the shipment table with proper schema."""
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
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
        self.spark.sql(create_sql)
    
    def get_schema(self) -> StructType:
        """Get the expected schema for shipment data."""
        return StructType([
            StructField("shipment_id", StringType(), False),
            StructField("route_id", StringType(), False),
            StructField("carrier_id", StringType(), False),
            StructField("origin", StringType(), False),
            StructField("destination", StringType(), False),
            StructField("planned_departure_ts", TimestampType(), True),
            StructField("actual_departure_ts", TimestampType(), True),
            StructField("planned_arrival_ts", TimestampType(), True),
            StructField("actual_arrival_ts", TimestampType(), True),
            StructField("status", StringType(), False),
            StructField("load_ts", TimestampType(), False),
            StructField("source_system", StringType(), False)
        ])
    
    def validate_shipment_data(self, df: DataFrame) -> DataFrame:
        """Validate and clean shipment data."""
        # Ensure required columns exist
        required_cols = [
            "shipment_id", "route_id", "carrier_id", "origin", 
            "destination", "status", "source_system"
        ]
        
        for col_name in required_cols:
            if col_name not in df.columns:
                raise ValueError(f"Required column {col_name} missing from shipment data")
        
        # Add load timestamp if not present
        if "load_ts" not in df.columns:
            df = df.withColumn("load_ts", current_timestamp())
        
        # Validate status values
        valid_statuses = ["planned", "in_transit", "delivered", "cancelled"]
        df = df.withColumn(
            "status",
            when(col("status").isin(valid_statuses), col("status"))
            .otherwise(lit("planned"))
        )
        
        # Validate timestamp relationships
        df = df.withColumn(
            "planned_departure_ts",
            when(
                (col("planned_departure_ts").isNotNull()) &
                (col("planned_arrival_ts").isNotNull()) &
                (col("planned_departure_ts") > col("planned_arrival_ts")),
                col("planned_arrival_ts")
            ).otherwise(col("planned_departure_ts"))
        )
        
        df = df.withColumn(
            "actual_departure_ts",
            when(
                (col("actual_departure_ts").isNotNull()) &
                (col("actual_arrival_ts").isNotNull()) &
                (col("actual_departure_ts") > col("actual_arrival_ts")),
                col("actual_arrival_ts")
            ).otherwise(col("actual_departure_ts"))
        )
        
        return df
    
    def upsert_shipments(self, new_data: DataFrame) -> None:
        """
        Upsert shipment data.
        
        Args:
            new_data: DataFrame with shipment records
        """
        # Validate and clean data
        validated_data = self.validate_shipment_data(new_data)
        
        # Write to table
        validated_data.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(self.table_name)
    
    def get_shipments_by_route(self, route_id: str) -> DataFrame:
        """Get all shipments for a specific route."""
        return (
            self.spark.table(self.table_name)
            .filter(col("route_id") == route_id)
            .orderBy(col("planned_departure_ts"))
        )
    
    def get_shipments_by_carrier(self, carrier_id: str, status: Optional[str] = None) -> DataFrame:
        """Get shipments by carrier, optionally filtered by status."""
        query = (
            self.spark.table(self.table_name)
            .filter(col("carrier_id") == carrier_id)
        )
        
        if status:
            query = query.filter(col("status") == status)
        
        return query.orderBy(col("planned_departure_ts"))
    
    def get_active_shipments(self) -> DataFrame:
        """Get all active (in_transit) shipments."""
        return (
            self.spark.table(self.table_name)
            .filter(col("status") == "in_transit")
            .orderBy(col("planned_departure_ts"))
        )
    
    def get_delayed_shipments(self, delay_threshold_hours: int = 24) -> DataFrame:
        """Get shipments that are delayed beyond threshold."""
        return (
            self.spark.table(self.table_name)
            .filter(
                (col("status") == "in_transit") &
                (col("actual_departure_ts").isNotNull()) &
                (col("planned_departure_ts").isNotNull()) &
                (col("actual_departure_ts") > col("planned_departure_ts") + 
                 lit(delay_threshold_hours * 3600))  # Convert hours to seconds
            )
        )
    
    def calculate_delivery_metrics(self) -> DataFrame:
        """Calculate delivery performance metrics."""
        return (
            self.spark.table(self.table_name)
            .filter(
                (col("status") == "delivered") &
                (col("actual_arrival_ts").isNotNull()) &
                (col("planned_arrival_ts").isNotNull())
            )
            .withColumn(
                "delivery_delay_hours",
                (col("actual_arrival_ts").cast("long") - col("planned_arrival_ts").cast("long")) / 3600
            )
            .withColumn(
                "on_time",
                when(col("delivery_delay_hours") <= 0, True).otherwise(False)
            )
            .groupBy("carrier_id")
            .agg({
                "shipment_id": "count",
                "on_time": "avg",
                "delivery_delay_hours": "avg"
            })
            .withColumnRenamed("count(shipment_id)", "total_shipments")
            .withColumnRenamed("avg(on_time)", "on_time_rate")
            .withColumnRenamed("avg(delivery_delay_hours)", "avg_delay_hours")
        )
    
    def validate_data_quality(self) -> Dict[str, Any]:
        """Validate data quality for shipment table."""
        df = self.spark.table(self.table_name)
        
        # Check for missing required fields
        missing_shipment_id = df.filter(
            col("shipment_id").isNull() | (col("shipment_id") == "")
        ).count()
        
        missing_carrier_id = df.filter(
            col("carrier_id").isNull() | (col("carrier_id") == "")
        ).count()
        
        # Check for invalid status values
        invalid_status = df.filter(
            ~col("status").isin(["planned", "in_transit", "delivered", "cancelled"])
        ).count()
        
        # Check for invalid timestamp relationships
        invalid_timestamps = df.filter(
            (col("planned_departure_ts").isNotNull()) &
            (col("planned_arrival_ts").isNotNull()) &
            (col("planned_departure_ts") > col("planned_arrival_ts"))
        ).count()
        
        return {
            "missing_shipment_id": missing_shipment_id,
            "missing_carrier_id": missing_carrier_id,
            "invalid_status": invalid_status,
            "invalid_timestamps": invalid_timestamps,
            "total_records": df.count(),
            "active_shipments": df.filter(col("status") == "in_transit").count()
        }


def create_shipment_model(spark: SparkSession) -> ShipmentModel:
    """Factory function to create ShipmentModel instance."""
    return ShipmentModel(spark)

