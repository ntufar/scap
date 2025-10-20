"""
Inventory Position model for Navigator Supply Chain Lakehouse.

Handles inventory snapshots with quantity tracking and safety stock management.
"""

from datetime import datetime, date
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, 
    coalesce, isnan, isnull, max as spark_max,
    sum as spark_sum, avg as spark_avg
)
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, TimestampType
from pyspark.sql.window import Window


@dataclass
class InventoryPositionRecord:
    """Inventory position record with audit fields."""
    item_id: str
    location_id: str
    snapshot_date: date
    quantity_on_hand: float
    safety_stock: float
    in_transit_qty: float
    load_ts: datetime
    source_system: str


class InventoryPositionModel:
    """Inventory position model implementation."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.table_name = "silver.inventory_position"
    
    def create_table(self) -> None:
        """Create the inventory position table with proper schema."""
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
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
        self.spark.sql(create_sql)
    
    def get_schema(self) -> StructType:
        """Get the expected schema for inventory position data."""
        return StructType([
            StructField("item_id", StringType(), False),
            StructField("location_id", StringType(), False),
            StructField("snapshot_date", DateType(), False),
            StructField("quantity_on_hand", DoubleType(), False),
            StructField("safety_stock", DoubleType(), False),
            StructField("in_transit_qty", DoubleType(), False),
            StructField("load_ts", TimestampType(), False),
            StructField("source_system", StringType(), False)
        ])
    
    def validate_inventory_data(self, df: DataFrame) -> DataFrame:
        """Validate and clean inventory position data."""
        # Ensure required columns exist
        required_cols = [
            "item_id", "location_id", "snapshot_date", 
            "quantity_on_hand", "safety_stock", "in_transit_qty", "source_system"
        ]
        
        for col_name in required_cols:
            if col_name not in df.columns:
                raise ValueError(f"Required column {col_name} missing from inventory data")
        
        # Add load timestamp if not present
        if "load_ts" not in df.columns:
            df = df.withColumn("load_ts", current_timestamp())
        
        # Ensure quantities are non-negative
        df = df.withColumn(
            "quantity_on_hand",
            when(col("quantity_on_hand") < 0, 0.0).otherwise(col("quantity_on_hand"))
        )
        
        df = df.withColumn(
            "safety_stock",
            when(col("safety_stock") < 0, 0.0).otherwise(col("safety_stock"))
        )
        
        df = df.withColumn(
            "in_transit_qty",
            when(col("in_transit_qty") < 0, 0.0).otherwise(col("in_transit_qty"))
        )
        
        # Handle null values
        df = df.fillna({
            "quantity_on_hand": 0.0,
            "safety_stock": 0.0,
            "in_transit_qty": 0.0
        })
        
        return df
    
    def upsert_inventory_positions(self, new_data: DataFrame) -> None:
        """
        Upsert inventory position data.
        
        Args:
            new_data: DataFrame with inventory position records
        """
        # Validate and clean data
        validated_data = self.validate_inventory_data(new_data)
        
        # Write to table
        validated_data.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(self.table_name)
    
    def get_latest_inventory(self, item_id: Optional[str] = None, location_id: Optional[str] = None) -> DataFrame:
        """Get latest inventory positions, optionally filtered by item or location."""
        df = self.spark.table(self.table_name)
        
        if item_id:
            df = df.filter(col("item_id") == item_id)
        if location_id:
            df = df.filter(col("location_id") == location_id)
        
        # Get latest snapshot per item/location
        window = Window.partitionBy("item_id", "location_id").orderBy(col("snapshot_date").desc())
        
        return (
            df.withColumn("row_num", row_number().over(window))
            .filter(col("row_num") == 1)
            .drop("row_num")
        )
    
    def get_inventory_by_date(self, snapshot_date: date) -> DataFrame:
        """Get inventory positions for a specific date."""
        return (
            self.spark.table(self.table_name)
            .filter(col("snapshot_date") == snapshot_date)
        )
    
    def get_inventory_history(self, item_id: str, location_id: str, days: int = 30) -> DataFrame:
        """Get inventory history for an item at a location over specified days."""
        cutoff_date = datetime.now().date() - timedelta(days=days)
        
        return (
            self.spark.table(self.table_name)
            .filter(
                (col("item_id") == item_id) &
                (col("location_id") == location_id) &
                (col("snapshot_date") >= cutoff_date)
            )
            .orderBy(col("snapshot_date"))
        )
    
    def get_stockout_risk_items(self, threshold_days: int = 7) -> DataFrame:
        """Get items at risk of stockout based on current inventory and safety stock."""
        latest_inventory = self.get_latest_inventory()
        
        return (
            latest_inventory
            .filter(
                (col("quantity_on_hand") <= col("safety_stock")) |
                (col("quantity_on_hand") == 0)
            )
            .withColumn(
                "stockout_risk",
                when(col("quantity_on_hand") == 0, "immediate")
                .when(col("quantity_on_hand") <= col("safety_stock"), "high")
                .otherwise("low")
            )
        )
    
    def calculate_inventory_metrics(self) -> DataFrame:
        """Calculate inventory performance metrics."""
        latest_inventory = self.get_latest_inventory()
        
        return (
            latest_inventory
            .withColumn(
                "stockout_risk",
                when(col("quantity_on_hand") == 0, "stockout")
                .when(col("quantity_on_hand") <= col("safety_stock"), "at_risk")
                .otherwise("healthy")
            )
            .groupBy("location_id")
            .agg({
                "item_id": "count",
                "quantity_on_hand": "sum",
                "safety_stock": "sum",
                "in_transit_qty": "sum",
                "stockout_risk": "count"
            })
            .withColumnRenamed("count(item_id)", "total_items")
            .withColumnRenamed("sum(quantity_on_hand)", "total_on_hand")
            .withColumnRenamed("sum(safety_stock)", "total_safety_stock")
            .withColumnRenamed("sum(in_transit_qty)", "total_in_transit")
        )
    
    def get_inventory_turnover(self, item_id: str, location_id: str, days: int = 30) -> Dict[str, float]:
        """Calculate inventory turnover for an item at a location."""
        history = self.get_inventory_history(item_id, location_id, days)
        
        if history.count() < 2:
            return {"turnover_rate": 0.0, "days_in_inventory": 0.0}
        
        # Calculate average inventory and turnover
        avg_inventory = (
            history
            .agg(spark_avg("quantity_on_hand"))
            .collect()[0][0]
        )
        
        # Simple turnover calculation (would need sales data for accurate calculation)
        # This is a placeholder - in practice, you'd need sales/consumption data
        turnover_rate = 1.0 / max(avg_inventory, 1.0) if avg_inventory > 0 else 0.0
        days_in_inventory = 1.0 / turnover_rate if turnover_rate > 0 else 0.0
        
        return {
            "turnover_rate": turnover_rate,
            "days_in_inventory": days_in_inventory,
            "avg_inventory": avg_inventory
        }
    
    def validate_data_quality(self) -> Dict[str, Any]:
        """Validate data quality for inventory position table."""
        df = self.spark.table(self.table_name)
        
        # Check for missing required fields
        missing_item_id = df.filter(
            col("item_id").isNull() | (col("item_id") == "")
        ).count()
        
        missing_location_id = df.filter(
            col("location_id").isNull() | (col("location_id") == "")
        ).count()
        
        # Check for negative quantities
        negative_quantities = df.filter(
            (col("quantity_on_hand") < 0) |
            (col("safety_stock") < 0) |
            (col("in_transit_qty") < 0)
        ).count()
        
        # Check for duplicate snapshots
        duplicate_snapshots = (
            df.groupBy("item_id", "location_id", "snapshot_date")
            .count()
            .filter(col("count") > 1)
            .count()
        )
        
        return {
            "missing_item_id": missing_item_id,
            "missing_location_id": missing_location_id,
            "negative_quantities": negative_quantities,
            "duplicate_snapshots": duplicate_snapshots,
            "total_records": df.count(),
            "unique_items": df.select("item_id").distinct().count(),
            "unique_locations": df.select("location_id").distinct().count()
        }


def create_inventory_position_model(spark: SparkSession) -> InventoryPositionModel:
    """Factory function to create InventoryPositionModel instance."""
    return InventoryPositionModel(spark)

