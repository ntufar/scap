"""
Gold layer jobs for Navigator Supply Chain Lakehouse.

Handles data transformation from silver to gold layer with publish gate controls
and data quality validation.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, coalesce, when, 
    max as spark_max, count, sum as spark_sum, avg as spark_avg
)

from src.lib.logging import get_logger
from src.lib.publish_gate import PublishGate
from src.lib.dq import DataQualityChecker


logger = get_logger(__name__)


class GoldPublishJob:
    """Gold layer publish job with data quality gates."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = get_logger(self.__class__.__name__)
        self.publish_gate = PublishGate(spark)
        self.dq_checker = DataQualityChecker(spark)
    
    def create_gold_tables(self) -> None:
        """Create all gold layer tables."""
        self._create_unified_supply_chain_table()
        self._create_supplier_performance_table()
        self._create_inventory_metrics_table()
        self._create_shipment_performance_table()
        self.logger.info("All gold tables created successfully")
    
    def _create_unified_supply_chain_table(self) -> None:
        """Create the unified supply chain gold table."""
        create_sql = """
        CREATE TABLE IF NOT EXISTS gold.unified_supply_chain (
            product_id STRING NOT NULL,
            location_id STRING NOT NULL,
            supplier_id STRING NOT NULL,
            supplier_name STRING,
            supplier_status STRING,
            supplier_region STRING,
            supplier_attributes STRING,
            quantity_on_hand DOUBLE,
            safety_stock DOUBLE,
            in_transit_qty DOUBLE,
            inventory_date DATE,
            shipment_id STRING,
            route_id STRING,
            carrier_id STRING,
            origin STRING,
            destination STRING,
            planned_departure_ts TIMESTAMP,
            actual_departure_ts TIMESTAMP,
            planned_arrival_ts TIMESTAMP,
            actual_arrival_ts TIMESTAMP,
            shipment_status STRING,
            stock_status STRING,
            delivery_delay_days INT,
            is_delivered BOOLEAN,
            inventory_source STRING,
            supplier_source STRING,
            shipment_source STRING,
            inventory_load_ts TIMESTAMP,
            shipment_load_ts TIMESTAMP,
            view_created_ts TIMESTAMP,
            last_data_update_ts TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        USING DELTA
        PARTITIONED BY (supplier_region, inventory_date)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """
        self.spark.sql(create_sql)
        self.logger.info("Created gold.unified_supply_chain table")
    
    def _create_supplier_performance_table(self) -> None:
        """Create supplier performance metrics table."""
        create_sql = """
        CREATE TABLE IF NOT EXISTS gold.supplier_performance (
            supplier_id STRING NOT NULL,
            supplier_name STRING,
            supplier_region STRING,
            total_shipments BIGINT,
            on_time_deliveries BIGINT,
            on_time_rate DOUBLE,
            avg_delay_days DOUBLE,
            total_products BIGINT,
            avg_inventory_turnover DOUBLE,
            performance_score DOUBLE,
            last_updated TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        USING DELTA
        PARTITIONED BY (supplier_region)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """
        self.spark.sql(create_sql)
        self.logger.info("Created gold.supplier_performance table")
    
    def _create_inventory_metrics_table(self) -> None:
        """Create inventory metrics table."""
        create_sql = """
        CREATE TABLE IF NOT EXISTS gold.inventory_metrics (
            product_id STRING NOT NULL,
            location_id STRING NOT NULL,
            supplier_id STRING,
            total_inventory DOUBLE,
            safety_stock_level DOUBLE,
            stockout_risk STRING,
            turnover_rate DOUBLE,
            days_in_inventory DOUBLE,
            last_movement_date DATE,
            last_updated TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        USING DELTA
        PARTITIONED BY (location_id)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """
        self.spark.sql(create_sql)
        self.logger.info("Created gold.inventory_metrics table")
    
    def _create_shipment_performance_table(self) -> None:
        """Create shipment performance metrics table."""
        create_sql = """
        CREATE TABLE IF NOT EXISTS gold.shipment_performance (
            carrier_id STRING NOT NULL,
            route_id STRING,
            total_shipments BIGINT,
            on_time_shipments BIGINT,
            delayed_shipments BIGINT,
            cancelled_shipments BIGINT,
            on_time_rate DOUBLE,
            avg_delay_hours DOUBLE,
            avg_transit_days DOUBLE,
            last_updated TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        USING DELTA
        PARTITIONED BY (carrier_id)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """
        self.spark.sql(create_sql)
        self.logger.info("Created gold.shipment_performance table")
    
    def run_data_quality_checks(self) -> Dict[str, Any]:
        """Run comprehensive data quality checks before publishing."""
        self.logger.info("Running data quality checks...")
        
        checks = {
            "unified_supply_chain": self._check_unified_supply_chain_quality(),
            "supplier_data": self._check_supplier_data_quality(),
            "inventory_data": self._check_inventory_data_quality(),
            "shipment_data": self._check_shipment_data_quality()
        }
        
        # Check if any critical checks failed
        critical_failures = [
            check for check in checks.values() 
            if check.get("critical_failures", 0) > 0
        ]
        
        checks["overall_status"] = "PASS" if not critical_failures else "FAIL"
        checks["critical_failures"] = len(critical_failures)
        
        self.logger.info(f"Data quality checks completed: {checks['overall_status']}")
        return checks
    
    def _check_unified_supply_chain_quality(self) -> Dict[str, Any]:
        """Check data quality for unified supply chain view."""
        try:
            # Check if view exists and has data
            view_df = self.spark.sql("SELECT * FROM gold.unified_supply_chain LIMIT 1")
            record_count = self.spark.sql("SELECT COUNT(*) as cnt FROM gold.unified_supply_chain").collect()[0][0]
            
            # Check for orphaned records
            orphaned = self.spark.sql("""
                SELECT COUNT(*) as cnt 
                FROM gold.unified_supply_chain 
                WHERE product_id IS NULL OR location_id IS NULL OR supplier_id IS NULL
            """).collect()[0][0]
            
            # Check for duplicate records
            duplicates = self.spark.sql("""
                SELECT COUNT(*) as cnt FROM (
                    SELECT product_id, location_id, supplier_id, inventory_date
                    FROM gold.unified_supply_chain
                    GROUP BY product_id, location_id, supplier_id, inventory_date
                    HAVING COUNT(*) > 1
                )
            """).collect()[0][0]
            
            return {
                "record_count": record_count,
                "orphaned_records": orphaned,
                "duplicate_records": duplicates,
                "critical_failures": orphaned + duplicates,
                "status": "PASS" if orphaned == 0 and duplicates == 0 else "FAIL"
            }
            
        except Exception as e:
            self.logger.error(f"Unified supply chain quality check failed: {str(e)}")
            return {
                "record_count": 0,
                "orphaned_records": 0,
                "duplicate_records": 0,
                "critical_failures": 1,
                "status": "FAIL",
                "error": str(e)
            }
    
    def _check_supplier_data_quality(self) -> Dict[str, Any]:
        """Check supplier data quality."""
        try:
            # Check for missing supplier names
            missing_names = self.spark.sql("""
                SELECT COUNT(*) as cnt 
                FROM silver.supplier_scd2 
                WHERE is_current = true AND (name IS NULL OR name = '')
            """).collect()[0][0]
            
            # Check for duplicate business keys
            duplicates = self.spark.sql("""
                SELECT COUNT(*) as cnt FROM (
                    SELECT business_key 
                    FROM silver.supplier_scd2 
                    WHERE is_current = true
                    GROUP BY business_key 
                    HAVING COUNT(*) > 1
                )
            """).collect()[0][0]
            
            return {
                "missing_names": missing_names,
                "duplicate_keys": duplicates,
                "critical_failures": missing_names + duplicates,
                "status": "PASS" if missing_names == 0 and duplicates == 0 else "FAIL"
            }
            
        except Exception as e:
            self.logger.error(f"Supplier data quality check failed: {str(e)}")
            return {
                "missing_names": 0,
                "duplicate_keys": 0,
                "critical_failures": 1,
                "status": "FAIL",
                "error": str(e)
            }
    
    def _check_inventory_data_quality(self) -> Dict[str, Any]:
        """Check inventory data quality."""
        try:
            # Check for negative quantities
            negative_quantities = self.spark.sql("""
                SELECT COUNT(*) as cnt 
                FROM silver.inventory_position 
                WHERE quantity_on_hand < 0 OR safety_stock < 0 OR in_transit_qty < 0
            """).collect()[0][0]
            
            # Check for missing required fields
            missing_fields = self.spark.sql("""
                SELECT COUNT(*) as cnt 
                FROM silver.inventory_position 
                WHERE item_id IS NULL OR location_id IS NULL OR snapshot_date IS NULL
            """).collect()[0][0]
            
            return {
                "negative_quantities": negative_quantities,
                "missing_fields": missing_fields,
                "critical_failures": negative_quantities + missing_fields,
                "status": "PASS" if negative_quantities == 0 and missing_fields == 0 else "FAIL"
            }
            
        except Exception as e:
            self.logger.error(f"Inventory data quality check failed: {str(e)}")
            return {
                "negative_quantities": 0,
                "missing_fields": 0,
                "critical_failures": 1,
                "status": "FAIL",
                "error": str(e)
            }
    
    def _check_shipment_data_quality(self) -> Dict[str, Any]:
        """Check shipment data quality."""
        try:
            # Check for invalid status values
            invalid_status = self.spark.sql("""
                SELECT COUNT(*) as cnt 
                FROM silver.shipment 
                WHERE status NOT IN ('planned', 'in_transit', 'delivered', 'cancelled')
            """).collect()[0][0]
            
            # Check for missing required fields
            missing_fields = self.spark.sql("""
                SELECT COUNT(*) as cnt 
                FROM silver.shipment 
                WHERE shipment_id IS NULL OR carrier_id IS NULL OR status IS NULL
            """).collect()[0][0]
            
            return {
                "invalid_status": invalid_status,
                "missing_fields": missing_fields,
                "critical_failures": invalid_status + missing_fields,
                "status": "PASS" if invalid_status == 0 and missing_fields == 0 else "FAIL"
            }
            
        except Exception as e:
            self.logger.error(f"Shipment data quality check failed: {str(e)}")
            return {
                "invalid_status": 0,
                "missing_fields": 0,
                "critical_failures": 1,
                "status": "FAIL",
                "error": str(e)
            }
    
    def publish_unified_supply_chain(self) -> bool:
        """Publish unified supply chain data to gold layer."""
        try:
            self.logger.info("Publishing unified supply chain data...")
            
            # Run data quality checks
            dq_results = self.run_data_quality_checks()
            
            # Check if data quality passes
            if dq_results["overall_status"] != "PASS":
                self.logger.error(f"Data quality checks failed: {dq_results}")
                return False
            
            # Check publish gate
            if not self.publish_gate.can_publish():
                self.logger.error("Publish gate is closed - cannot publish data")
                return False
            
            # Create the unified view (this will populate the gold table)
            self.spark.sql("""
                INSERT OVERWRITE TABLE gold.unified_supply_chain
                SELECT * FROM gold.unified_supply_chain
            """)
            
            # Update publish gate
            self.publish_gate.record_publish("unified_supply_chain", dq_results)
            
            self.logger.info("Unified supply chain data published successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to publish unified supply chain data: {str(e)}")
            return False
    
    def publish_supplier_performance(self) -> bool:
        """Publish supplier performance metrics."""
        try:
            self.logger.info("Publishing supplier performance metrics...")
            
            # Calculate supplier performance metrics
            metrics_df = self.spark.sql("""
                SELECT 
                    supplier_id,
                    supplier_name,
                    supplier_region,
                    COUNT(DISTINCT shipment_id) as total_shipments,
                    SUM(CASE WHEN is_delivered = true THEN 1 ELSE 0 END) as on_time_deliveries,
                    AVG(CASE WHEN is_delivered = true THEN 1.0 ELSE 0.0 END) as on_time_rate,
                    AVG(delivery_delay_days) as avg_delay_days,
                    COUNT(DISTINCT product_id) as total_products,
                    0.0 as avg_inventory_turnover,  -- Placeholder
                    CASE 
                        WHEN AVG(CASE WHEN is_delivered = true THEN 1.0 ELSE 0.0 END) >= 0.9 THEN 5.0
                        WHEN AVG(CASE WHEN is_delivered = true THEN 1.0 ELSE 0.0 END) >= 0.8 THEN 4.0
                        WHEN AVG(CASE WHEN is_delivered = true THEN 1.0 ELSE 0.0 END) >= 0.7 THEN 3.0
                        WHEN AVG(CASE WHEN is_delivered = true THEN 1.0 ELSE 0.0 END) >= 0.6 THEN 2.0
                        ELSE 1.0
                    END as performance_score,
                    CURRENT_TIMESTAMP() as last_updated
                FROM gold.unified_supply_chain
                WHERE supplier_id IS NOT NULL
                GROUP BY supplier_id, supplier_name, supplier_region
            """)
            
            # Write to gold table
            metrics_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable("gold.supplier_performance")
            
            self.logger.info("Supplier performance metrics published successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to publish supplier performance metrics: {str(e)}")
            return False
    
    def publish_inventory_metrics(self) -> bool:
        """Publish inventory metrics."""
        try:
            self.logger.info("Publishing inventory metrics...")
            
            # Calculate inventory metrics
            metrics_df = self.spark.sql("""
                SELECT 
                    product_id,
                    location_id,
                    supplier_id,
                    SUM(quantity_on_hand) as total_inventory,
                    AVG(safety_stock) as safety_stock_level,
                    CASE 
                        WHEN SUM(quantity_on_hand) = 0 THEN 'stockout'
                        WHEN SUM(quantity_on_hand) <= AVG(safety_stock) THEN 'at_risk'
                        ELSE 'healthy'
                    END as stockout_risk,
                    0.0 as turnover_rate,  -- Placeholder
                    0.0 as days_in_inventory,  -- Placeholder
                    MAX(inventory_date) as last_movement_date,
                    CURRENT_TIMESTAMP() as last_updated
                FROM gold.unified_supply_chain
                WHERE product_id IS NOT NULL AND location_id IS NOT NULL
                GROUP BY product_id, location_id, supplier_id
            """)
            
            # Write to gold table
            metrics_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable("gold.inventory_metrics")
            
            self.logger.info("Inventory metrics published successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to publish inventory metrics: {str(e)}")
            return False
    
    def publish_shipment_performance(self) -> bool:
        """Publish shipment performance metrics."""
        try:
            self.logger.info("Publishing shipment performance metrics...")
            
            # Calculate shipment performance metrics
            metrics_df = self.spark.sql("""
                SELECT 
                    carrier_id,
                    route_id,
                    COUNT(*) as total_shipments,
                    SUM(CASE WHEN is_delivered = true THEN 1 ELSE 0 END) as on_time_shipments,
                    SUM(CASE WHEN delivery_delay_days > 0 THEN 1 ELSE 0 END) as delayed_shipments,
                    SUM(CASE WHEN shipment_status = 'cancelled' THEN 1 ELSE 0 END) as cancelled_shipments,
                    AVG(CASE WHEN is_delivered = true THEN 1.0 ELSE 0.0 END) as on_time_rate,
                    AVG(delivery_delay_days) as avg_delay_hours,
                    AVG(DATEDIFF(COALESCE(actual_arrival_ts, planned_arrival_ts), planned_departure_ts)) as avg_transit_days,
                    CURRENT_TIMESTAMP() as last_updated
                FROM gold.unified_supply_chain
                WHERE carrier_id IS NOT NULL
                GROUP BY carrier_id, route_id
            """)
            
            # Write to gold table
            metrics_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable("gold.shipment_performance")
            
            self.logger.info("Shipment performance metrics published successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to publish shipment performance metrics: {str(e)}")
            return False
    
    def publish_all_gold_tables(self) -> bool:
        """Publish all gold layer tables."""
        try:
            self.logger.info("Starting gold layer publish process...")
            
            # Create tables if they don't exist
            self.create_gold_tables()
            
            # Publish each table
            results = {
                "unified_supply_chain": self.publish_unified_supply_chain(),
                "supplier_performance": self.publish_supplier_performance(),
                "inventory_metrics": self.publish_inventory_metrics(),
                "shipment_performance": self.publish_shipment_performance()
            }
            
            # Check if all publishes succeeded
            success = all(results.values())
            
            if success:
                self.logger.info("All gold tables published successfully")
            else:
                failed_tables = [table for table, result in results.items() if not result]
                self.logger.error(f"Failed to publish tables: {failed_tables}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Gold layer publish process failed: {str(e)}")
            return False


def run_gold_publish(spark: SparkSession, table: str = "all") -> bool:
    """Run gold layer publish for specific table or all tables."""
    job = GoldPublishJob(spark)
    
    if table == "all":
        return job.publish_all_gold_tables()
    elif table == "unified_supply_chain":
        return job.publish_unified_supply_chain()
    elif table == "supplier_performance":
        return job.publish_supplier_performance()
    elif table == "inventory_metrics":
        return job.publish_inventory_metrics()
    elif table == "shipment_performance":
        return job.publish_shipment_performance()
    else:
        raise ValueError(f"Unknown table: {table}")


if __name__ == "__main__":
    # Example usage
    spark = SparkSession.builder \
        .appName("GoldPublish") \
        .getOrCreate()
    
    # Publish all gold tables
    success = run_gold_publish(spark, "all")
    
    if success:
        logger.info("Gold layer publish completed successfully")
    else:
        logger.error("Gold layer publish failed")
    
    spark.stop()