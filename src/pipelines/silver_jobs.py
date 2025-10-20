"""
Silver conformance jobs for Navigator Supply Chain Lakehouse.

Handles data transformation from bronze to silver layer with key resolution,
crosswalks, and SCD2 processing.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, coalesce, when, 
    row_number, max as spark_max, first, last
)
from pyspark.sql.window import Window

from src.lib.logging import get_logger
from src.lib.publish_gate import PublishGate
from src.models.crosswalk import CrosswalkModel
from src.models.identity_resolution import IdentityResolutionModel
from src.models.supplier import SupplierSCD2, create_supplier_scd2
from src.models.shipment import ShipmentModel, create_shipment_model
from src.models.inventory_position import InventoryPositionModel, create_inventory_position_model


logger = get_logger(__name__)


class SilverConformanceJob:
    """Base class for silver conformance jobs."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = get_logger(self.__class__.__name__)
        self.publish_gate = PublishGate(spark)
        self.crosswalk_model = CrosswalkModel(spark)
        self.identity_resolution = IdentityResolutionModel(spark)
    
    def validate_bronze_data(self, bronze_table: str, required_columns: List[str]) -> bool:
        """Validate bronze data before processing."""
        try:
            df = self.spark.table(bronze_table)
            
            # Check required columns exist
            missing_cols = [col for col in required_columns if col not in df.columns]
            if missing_cols:
                self.logger.error(f"Missing required columns in {bronze_table}: {missing_cols}")
                return False
            
            # Check for empty data
            if df.count() == 0:
                self.logger.warning(f"No data found in {bronze_table}")
                return False
            
            self.logger.info(f"Bronze data validation passed for {bronze_table}")
            return True
            
        except Exception as e:
            self.logger.error(f"Bronze data validation failed for {bronze_table}: {str(e)}")
            return False


class SupplierSilverJob(SilverConformanceJob):
    """Silver conformance job for supplier data with SCD2 processing."""
    
    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self.bronze_table = "bronze.supplier_raw"
        self.silver_table = "silver.supplier_scd2"
        self.supplier_scd2 = create_supplier_scd2(spark)
    
    def process_suppliers(self) -> bool:
        """Process supplier data from bronze to silver with SCD2."""
        try:
            self.logger.info("Starting supplier silver conformance processing")
            
            # Validate bronze data
            required_columns = ["supplier_id", "supplier_name", "status", "region"]
            if not self.validate_bronze_data(self.bronze_table, required_columns):
                return False
            
            # Read bronze data
            bronze_df = self.spark.table(self.bronze_table)
            
            # Transform and clean data
            transformed_df = self._transform_supplier_data(bronze_df)
            
            # Apply identity resolution
            resolved_df = self._apply_identity_resolution(transformed_df)
            
            # Process SCD2 updates
            self.supplier_scd2.create_table()
            self.supplier_scd2.upsert_suppliers(resolved_df)
            
            # Validate data quality
            dq_results = self.supplier_scd2.validate_data_quality()
            self.logger.info(f"Supplier data quality results: {dq_results}")
            
            # Check if data quality passes
            if dq_results["orphaned_records"] > 0 or dq_results["duplicate_current"] > 0:
                self.logger.error("Data quality issues found in supplier data")
                return False
            
            self.logger.info("Supplier silver conformance completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Supplier silver conformance failed: {str(e)}")
            return False
    
    def _transform_supplier_data(self, df: DataFrame) -> DataFrame:
        """Transform bronze supplier data to silver format."""
        return df.select(
            col("supplier_id").alias("natural_key"),
            col("supplier_id").alias("business_key"),  # Will be resolved by identity resolution
            col("supplier_name").alias("name"),
            col("status"),
            col("region"),
            coalesce(col("attributes"), lit("{}")).alias("attributes_json"),
            col("_source_system").alias("source_system"),
            col("_ingestion_ts").alias("load_ts")
        )
    
    def _apply_identity_resolution(self, df: DataFrame) -> DataFrame:
        """Apply identity resolution to supplier data."""
        # For now, use natural key as business key
        # In a real implementation, this would use the crosswalk model
        return df.withColumn("business_key", col("natural_key"))


class ShipmentSilverJob(SilverConformanceJob):
    """Silver conformance job for shipment data."""
    
    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self.bronze_table = "bronze.shipment_raw"
        self.silver_table = "silver.shipment"
        self.shipment_model = create_shipment_model(spark)
    
    def process_shipments(self) -> bool:
        """Process shipment data from bronze to silver."""
        try:
            self.logger.info("Starting shipment silver conformance processing")
            
            # Validate bronze data
            required_columns = ["shipment_id", "route_id", "carrier_id", "origin", "destination", "status"]
            if not self.validate_bronze_data(self.bronze_table, required_columns):
                return False
            
            # Read bronze data
            bronze_df = self.spark.table(self.bronze_table)
            
            # Transform and clean data
            transformed_df = self._transform_shipment_data(bronze_df)
            
            # Apply business key resolution
            resolved_df = self._apply_business_key_resolution(transformed_df)
            
            # Process shipment updates
            self.shipment_model.create_table()
            self.shipment_model.upsert_shipments(resolved_df)
            
            # Validate data quality
            dq_results = self.shipment_model.validate_data_quality()
            self.logger.info(f"Shipment data quality results: {dq_results}")
            
            # Check if data quality passes
            if dq_results["missing_shipment_id"] > 0 or dq_results["invalid_status"] > 0:
                self.logger.error("Data quality issues found in shipment data")
                return False
            
            self.logger.info("Shipment silver conformance completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Shipment silver conformance failed: {str(e)}")
            return False
    
    def _transform_shipment_data(self, df: DataFrame) -> DataFrame:
        """Transform bronze shipment data to silver format."""
        return df.select(
            col("shipment_id"),
            col("route_id"),
            col("carrier_id"),
            col("origin"),
            col("destination"),
            col("planned_departure").alias("planned_departure_ts"),
            col("actual_departure").alias("actual_departure_ts"),
            col("planned_arrival").alias("planned_arrival_ts"),
            col("actual_arrival").alias("actual_arrival_ts"),
            col("status"),
            col("_source_system").alias("source_system"),
            col("_ingestion_ts").alias("load_ts")
        )
    
    def _apply_business_key_resolution(self, df: DataFrame) -> DataFrame:
        """Apply business key resolution for shipments."""
        # For now, use shipment_id as business key
        # In a real implementation, this would use the crosswalk model
        return df


class InventorySilverJob(SilverConformanceJob):
    """Silver conformance job for inventory data."""
    
    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self.bronze_table = "bronze.inventory_raw"
        self.silver_table = "silver.inventory_position"
        self.inventory_model = create_inventory_position_model(spark)
    
    def process_inventory(self) -> bool:
        """Process inventory data from bronze to silver."""
        try:
            self.logger.info("Starting inventory silver conformance processing")
            
            # Validate bronze data
            required_columns = ["item_id", "location_id", "snapshot_date", "quantity_on_hand"]
            if not self.validate_bronze_data(self.bronze_table, required_columns):
                return False
            
            # Read bronze data
            bronze_df = self.spark.table(self.bronze_table)
            
            # Transform and clean data
            transformed_df = self._transform_inventory_data(bronze_df)
            
            # Apply business key resolution
            resolved_df = self._apply_business_key_resolution(transformed_df)
            
            # Process inventory updates
            self.inventory_model.create_table()
            self.inventory_model.upsert_inventory_positions(resolved_df)
            
            # Validate data quality
            dq_results = self.inventory_model.validate_data_quality()
            self.logger.info(f"Inventory data quality results: {dq_results}")
            
            # Check if data quality passes
            if dq_results["missing_item_id"] > 0 or dq_results["negative_quantities"] > 0:
                self.logger.error("Data quality issues found in inventory data")
                return False
            
            self.logger.info("Inventory silver conformance completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Inventory silver conformance failed: {str(e)}")
            return False
    
    def _transform_inventory_data(self, df: DataFrame) -> DataFrame:
        """Transform bronze inventory data to silver format."""
        return df.select(
            col("item_id"),
            col("location_id"),
            col("snapshot_date"),
            col("quantity_on_hand"),
            coalesce(col("safety_stock"), lit(0.0)).alias("safety_stock"),
            coalesce(col("in_transit_qty"), lit(0.0)).alias("in_transit_qty"),
            col("_source_system").alias("source_system"),
            col("_ingestion_ts").alias("load_ts")
        )
    
    def _apply_business_key_resolution(self, df: DataFrame) -> DataFrame:
        """Apply business key resolution for inventory."""
        # For now, use item_id as business key
        # In a real implementation, this would use the crosswalk model
        return df


class LateArrivingUpdatesHandler:
    """Handles late-arriving updates for silver layer data."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = get_logger(self.__class__.__name__)
    
    def process_late_arriving_supplier_updates(self, cutoff_hours: int = 24) -> bool:
        """Process late-arriving supplier updates."""
        try:
            self.logger.info(f"Processing late-arriving supplier updates (cutoff: {cutoff_hours}h)")
            
            # Get recent bronze data
            cutoff_time = datetime.now() - timedelta(hours=cutoff_hours)
            recent_bronze = self.spark.table("bronze.supplier_raw") \
                .filter(col("_ingestion_ts") >= cutoff_time)
            
            if recent_bronze.count() == 0:
                self.logger.info("No late-arriving supplier updates found")
                return True
            
            # Process updates
            supplier_job = SupplierSilverJob(self.spark)
            return supplier_job.process_suppliers()
            
        except Exception as e:
            self.logger.error(f"Late-arriving supplier updates failed: {str(e)}")
            return False
    
    def process_late_arriving_shipment_updates(self, cutoff_hours: int = 24) -> bool:
        """Process late-arriving shipment updates."""
        try:
            self.logger.info(f"Processing late-arriving shipment updates (cutoff: {cutoff_hours}h)")
            
            # Get recent bronze data
            cutoff_time = datetime.now() - timedelta(hours=cutoff_hours)
            recent_bronze = self.spark.table("bronze.shipment_raw") \
                .filter(col("_ingestion_ts") >= cutoff_time)
            
            if recent_bronze.count() == 0:
                self.logger.info("No late-arriving shipment updates found")
                return True
            
            # Process updates
            shipment_job = ShipmentSilverJob(self.spark)
            return shipment_job.process_shipments()
            
        except Exception as e:
            self.logger.error(f"Late-arriving shipment updates failed: {str(e)}")
            return False
    
    def process_late_arriving_inventory_updates(self, cutoff_hours: int = 24) -> bool:
        """Process late-arriving inventory updates."""
        try:
            self.logger.info(f"Processing late-arriving inventory updates (cutoff: {cutoff_hours}h)")
            
            # Get recent bronze data
            cutoff_time = datetime.now() - timedelta(hours=cutoff_hours)
            recent_bronze = self.spark.table("bronze.inventory_raw") \
                .filter(col("_ingestion_ts") >= cutoff_time)
            
            if recent_bronze.count() == 0:
                self.logger.info("No late-arriving inventory updates found")
                return True
            
            # Process updates
            inventory_job = InventorySilverJob(self.spark)
            return inventory_job.process_inventory()
            
        except Exception as e:
            self.logger.error(f"Late-arriving inventory updates failed: {str(e)}")
            return False


def run_silver_conformance(spark: SparkSession, domain: str) -> bool:
    """Run silver conformance for a specific domain."""
    try:
        if domain == "supplier":
            job = SupplierSilverJob(spark)
            return job.process_suppliers()
        elif domain == "shipment":
            job = ShipmentSilverJob(spark)
            return job.process_shipments()
        elif domain == "inventory":
            job = InventorySilverJob(spark)
            return job.process_inventory()
        elif domain == "all":
            # Process all domains
            supplier_job = SupplierSilverJob(spark)
            shipment_job = ShipmentSilverJob(spark)
            inventory_job = InventorySilverJob(spark)
            
            results = {
                "supplier": supplier_job.process_suppliers(),
                "shipment": shipment_job.process_shipments(),
                "inventory": inventory_job.process_inventory()
            }
            
            logger.info(f"Silver conformance results: {results}")
            return all(results.values())
        else:
            raise ValueError(f"Unknown domain: {domain}")
            
    except Exception as e:
        logger.error(f"Silver conformance failed for domain {domain}: {str(e)}")
        return False


def run_late_arriving_updates(spark: SparkSession, domain: str, cutoff_hours: int = 24) -> bool:
    """Run late-arriving updates processing for a specific domain."""
    handler = LateArrivingUpdatesHandler(spark)
    
    if domain == "supplier":
        return handler.process_late_arriving_supplier_updates(cutoff_hours)
    elif domain == "shipment":
        return handler.process_late_arriving_shipment_updates(cutoff_hours)
    elif domain == "inventory":
        return handler.process_late_arriving_inventory_updates(cutoff_hours)
    elif domain == "all":
        results = {
            "supplier": handler.process_late_arriving_supplier_updates(cutoff_hours),
            "shipment": handler.process_late_arriving_shipment_updates(cutoff_hours),
            "inventory": handler.process_late_arriving_inventory_updates(cutoff_hours)
        }
        logger.info(f"Late-arriving updates results: {results}")
        return all(results.values())
    else:
        raise ValueError(f"Unknown domain: {domain}")


if __name__ == "__main__":
    # Example usage
    spark = SparkSession.builder \
        .appName("SilverConformance") \
        .getOrCreate()
    
    # Run silver conformance for all domains
    success = run_silver_conformance(spark, "all")
    
    if success:
        logger.info("Silver conformance completed successfully")
    else:
        logger.error("Silver conformance failed")
    
    spark.stop()