"""
CLI entry points for Navigator Supply Chain Lakehouse.

Provides command-line interface for data simulation, loading, and pipeline execution.
"""

import argparse
import sys
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

from pyspark.sql import SparkSession

from src.lib.logging import get_logger
from src.pipelines.bronze_jobs import run_bronze_ingestion, create_bronze_jobs
from src.pipelines.silver_jobs import run_silver_conformance, run_late_arriving_updates
from src.pipelines.gold_jobs import run_gold_publish
from src.pipelines.setup_uc import setup_unity_catalog


logger = get_logger(__name__)


class SupplyChainCLI:
    """Command-line interface for supply chain data operations."""
    
    def __init__(self):
        self.spark = None
        self.logger = get_logger(self.__class__.__name__)
    
    def init_spark(self, app_name: str = "SupplyChainCLI") -> None:
        """Initialize Spark session for Databricks environment."""
        if self.spark is None:
            # In Databricks, Spark session is already available
            try:
                from pyspark.sql import SparkSession
                self.spark = SparkSession.getActiveSession()
                if self.spark is None:
                    self.spark = SparkSession.builder.appName(app_name).getOrCreate()
                self.logger.info("Spark session initialized for Databricks")
            except Exception as e:
                self.logger.error(f"Failed to initialize Spark session: {str(e)}")
                self.logger.error("Make sure you're running this in a Databricks environment")
                raise
    
    def cleanup(self) -> None:
        """Cleanup resources."""
        if self.spark:
            self.spark.stop()
            self.spark = None
            self.logger.info("Spark session stopped")


def simulate_data(args) -> None:
    """Simulate sample data for testing."""
    cli = SupplyChainCLI()
    try:
        cli.init_spark("DataSimulation")
        
        domain = args.domain
        output_path = args.output_path
        record_count = args.record_count
        
        # Convert paths to Databricks DBFS format
        if not output_path.startswith('/dbfs/') and not output_path.startswith('dbfs:'):
            if output_path.startswith('/tmp/'):
                output_path = output_path.replace('/tmp/', '/dbfs/tmp/')
            elif not output_path.startswith('/'):
                output_path = f"/dbfs/tmp/{output_path}"
        
        cli.logger.info(f"Simulating {record_count} records for domain: {domain}")
        
        if domain == "supplier":
            _simulate_supplier_data(cli.spark, output_path, record_count)
        elif domain == "shipment":
            _simulate_shipment_data(cli.spark, output_path, record_count)
        elif domain == "inventory":
            _simulate_inventory_data(cli.spark, output_path, record_count)
        else:
            raise ValueError(f"Unknown domain: {domain}")
        
        cli.logger.info(f"Data simulation completed. Output: {output_path}")
        
    except Exception as e:
        cli.logger.error(f"Data simulation failed: {str(e)}")
        sys.exit(1)
    finally:
        cli.cleanup()


def _simulate_supplier_data(spark: SparkSession, output_path: str, count: int) -> None:
    """Simulate supplier data."""
    from pyspark.sql.functions import lit, current_timestamp, rand, when
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    
    # Create sample supplier data
    suppliers = []
    regions = ["North America", "Europe", "Asia Pacific", "Latin America"]
    statuses = ["active", "inactive", "pending"]
    
    for i in range(count):
        suppliers.append({
            "supplier_id": f"SUP_{i+1:06d}",
            "supplier_name": f"Supplier {i+1}",
            "status": statuses[i % len(statuses)],
            "region": regions[i % len(regions)],
            "contact_email": f"contact{i+1}@supplier{i+1}.com",
            "contact_phone": f"+1-555-{i+1:04d}",
            "address": f"{i+1} Main St, City {i+1}",
            "attributes": f'{{"category": "electronics", "tier": {(i % 3) + 1}}}',
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    
    # Create DataFrame and write
    schema = StructType([
        StructField("supplier_id", StringType(), False),
        StructField("supplier_name", StringType(), False),
        StructField("status", StringType(), False),
        StructField("region", StringType(), True),
        StructField("contact_email", StringType(), True),
        StructField("contact_phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("attributes", StringType(), True),
        StructField("last_updated", StringType(), True)
    ])
    
    df = spark.createDataFrame(suppliers, schema)
    # Write as Delta table for Databricks
    df.write.format("delta").mode("overwrite").save(output_path)


def _simulate_shipment_data(spark: SparkSession, output_path: str, count: int) -> None:
    """Simulate shipment data."""
    from pyspark.sql.functions import lit, current_timestamp, rand, when
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
    
    # Create sample shipment data
    shipments = []
    statuses = ["planned", "in_transit", "delivered", "cancelled"]
    origins = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
    destinations = ["Miami", "Seattle", "Denver", "Boston", "Atlanta"]
    
    for i in range(count):
        shipments.append({
            "shipment_id": f"SHIP_{i+1:06d}",
            "route_id": f"ROUTE_{i+1:04d}",
            "carrier_id": f"CARR_{(i % 10) + 1:03d}",
            "origin": origins[i % len(origins)],
            "destination": destinations[i % len(destinations)],
            "planned_departure": (datetime.now() + timedelta(days=i)).strftime("%Y-%m-%d %H:%M:%S"),
            "actual_departure": (datetime.now() + timedelta(days=i, hours=1)).strftime("%Y-%m-%d %H:%M:%S"),
            "planned_arrival": (datetime.now() + timedelta(days=i+2)).strftime("%Y-%m-%d %H:%M:%S"),
            "actual_arrival": (datetime.now() + timedelta(days=i+2, hours=2)).strftime("%Y-%m-%d %H:%M:%S"),
            "status": statuses[i % len(statuses)],
            "tracking_number": f"TRK{i+1:08d}",
            "weight": 100.0 + (i * 10),
            "volume": 50.0 + (i * 5)
        })
    
    # Create DataFrame and write
    schema = StructType([
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
    
    df = spark.createDataFrame(shipments, schema)
    # Write as Delta table for Databricks
    df.write.format("delta").mode("overwrite").save(output_path)


def _simulate_inventory_data(spark: SparkSession, output_path: str, count: int) -> None:
    """Simulate inventory data."""
    from pyspark.sql.functions import lit, current_timestamp, rand, when
    from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType
    
    # Create sample inventory data
    inventory = []
    locations = ["WH001", "WH002", "WH003", "WH004", "WH005"]
    
    for i in range(count):
        inventory.append({
            "item_id": f"ITEM_{i+1:06d}",
            "location_id": locations[i % len(locations)],
            "snapshot_date": (datetime.now() - timedelta(days=i % 30)).strftime("%Y-%m-%d"),
            "quantity_on_hand": 100.0 + (i * 10),
            "safety_stock": 20.0 + (i * 2),
            "in_transit_qty": 10.0 + (i * 1),
            "reserved_qty": 5.0 + (i * 0.5),
            "available_qty": 95.0 + (i * 9.5),
            "unit_cost": 10.0 + (i * 0.1),
            "last_count_date": (datetime.now() - timedelta(days=i % 7)).strftime("%Y-%m-%d")
        })
    
    # Create DataFrame and write
    schema = StructType([
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
    
    df = spark.createDataFrame(inventory, schema)
    # Write as Delta table for Databricks
    df.write.format("delta").mode("overwrite").save(output_path)


def load_data(args) -> None:
    """Load data from source to bronze layer."""
    cli = SupplyChainCLI()
    try:
        cli.init_spark("DataLoading")
        
        domain = args.domain
        source_path = args.source_path
        
        cli.logger.info(f"Loading data for domain: {domain} from {source_path}")
        
        # Run bronze ingestion
        run_bronze_ingestion(cli.spark, domain, source_path)
        
        cli.logger.info("Data loading completed successfully")
        
    except Exception as e:
        cli.logger.error(f"Data loading failed: {str(e)}")
        sys.exit(1)
    finally:
        cli.cleanup()


def run_pipeline(args) -> None:
    """Run data pipeline for specified stage."""
    cli = SupplyChainCLI()
    try:
        cli.init_spark("PipelineExecution")
        
        stage = args.stage
        domain = args.domain if hasattr(args, 'domain') else "all"
        
        cli.logger.info(f"Running pipeline stage: {stage} for domain: {domain}")
        
        if stage == "bronze":
            # Create bronze tables
            jobs = create_bronze_jobs(cli.spark)
            for job in jobs.values():
                job.create_table()
            cli.logger.info("Bronze tables created")
            
        elif stage == "silver":
            # Run silver conformance
            success = run_silver_conformance(cli.spark, domain)
            if not success:
                raise Exception("Silver conformance failed")
            cli.logger.info("Silver conformance completed")
            
        elif stage == "gold":
            # Run gold publish
            success = run_gold_publish(cli.spark, "all")
            if not success:
                raise Exception("Gold publish failed")
            cli.logger.info("Gold publish completed")
            
        elif stage == "all":
            # Run full pipeline
            cli.logger.info("Running full pipeline...")
            
            # Bronze
            jobs = create_bronze_jobs(cli.spark)
            for job in jobs.values():
                job.create_table()
            cli.logger.info("Bronze stage completed")
            
            # Silver
            success = run_silver_conformance(cli.spark, "all")
            if not success:
                raise Exception("Silver conformance failed")
            cli.logger.info("Silver stage completed")
            
            # Gold
            success = run_gold_publish(cli.spark, "all")
            if not success:
                raise Exception("Gold publish failed")
            cli.logger.info("Gold stage completed")
            
        else:
            raise ValueError(f"Unknown stage: {stage}")
        
        cli.logger.info("Pipeline execution completed successfully")
        
    except Exception as e:
        cli.logger.error(f"Pipeline execution failed: {str(e)}")
        sys.exit(1)
    finally:
        cli.cleanup()


def setup_environment(args) -> None:
    """Setup Databricks environment."""
    cli = SupplyChainCLI()
    try:
        cli.init_spark("EnvironmentSetup")
        
        cli.logger.info("Setting up Databricks environment...")
        
        # Setup Databricks environment
        from src.pipelines.setup_databricks import setup_databricks_environment
        setup_databricks_environment(cli.spark)
        
        cli.logger.info("Databricks environment setup completed successfully")
        
    except Exception as e:
        cli.logger.error(f"Environment setup failed: {str(e)}")
        cli.logger.error("Make sure you're running this in a Databricks environment")
        sys.exit(1)
    finally:
        cli.cleanup()


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Navigator Supply Chain Lakehouse CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Simulate sample data
  python -m src.cli simulate --domain supplier --output-path /tmp/supplier_data --record-count 1000
  
  # Load data to bronze
  python -m src.cli load --domain supplier --source-path /tmp/supplier_data
  
  # Run pipeline stages
  python -m src.cli run --stage bronze
  python -m src.cli run --stage silver --domain supplier
  python -m src.cli run --stage gold
  python -m src.cli run --stage all
  
  # Setup environment
  python -m src.cli setup
        """
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Simulate command
    simulate_parser = subparsers.add_parser("simulate", help="Simulate sample data")
    simulate_parser.add_argument("--domain", required=True, choices=["supplier", "shipment", "inventory"],
                               help="Data domain to simulate")
    simulate_parser.add_argument("--output-path", required=True, help="Output path for simulated data")
    simulate_parser.add_argument("--record-count", type=int, default=1000, help="Number of records to generate")
    simulate_parser.set_defaults(func=simulate_data)
    
    # Load command
    load_parser = subparsers.add_parser("load", help="Load data to bronze layer")
    load_parser.add_argument("--domain", required=True, choices=["supplier", "shipment", "inventory"],
                           help="Data domain to load")
    load_parser.add_argument("--source-path", required=True, help="Source path for data")
    load_parser.set_defaults(func=load_data)
    
    # Run command
    run_parser = subparsers.add_parser("run", help="Run data pipeline")
    run_parser.add_argument("--stage", required=True, choices=["bronze", "silver", "gold", "all"],
                          help="Pipeline stage to run")
    run_parser.add_argument("--domain", choices=["supplier", "shipment", "inventory", "all"],
                          default="all", help="Data domain to process")
    run_parser.set_defaults(func=run_pipeline)
    
    # Setup command
    setup_parser = subparsers.add_parser("setup", help="Setup Unity Catalog environment")
    setup_parser.set_defaults(func=setup_environment)
    
    # Parse arguments
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Execute command
    try:
        args.func(args)
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Command failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()