#!/usr/bin/env python3
"""
Entry point for Databricks jobs: Setup Databricks environment for Navigator Supply Chain Lakehouse.
Can be executed directly or as a Databricks job.
"""

import sys
import os
import argparse
from pyspark.sql import SparkSession

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.pipelines.setup_databricks import setup_databricks_environment, initialize_sample_data


def main():
    """Main entry point for Databricks job execution."""
    parser = argparse.ArgumentParser(description="Setup Databricks environment for Navigator Supply Chain Lakehouse")
    parser.add_argument("--skip-sample-data", action="store_true", help="Skip sample data initialization")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    print("🚀 Setting up Databricks environment for Navigator Supply Chain Lakehouse...")
    
    # Get or create Spark session
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.appName("DatabricksSetup").getOrCreate()
    
    if args.verbose:
        print(f"✅ Spark session created: {spark.version}")
        print(f"✅ Spark context: {spark.sparkContext.applicationName}")
    
    try:
        # Setup the environment
        print("📋 Setting up Unity Catalog environment...")
        setup_databricks_environment(spark)
        print("✅ Unity Catalog environment setup completed!")
        
        # Initialize sample data (unless skipped)
        if not args.skip_sample_data:
            print("📊 Initializing sample data...")
            initialize_sample_data(spark)
            print("✅ Sample data initialization completed!")
        else:
            print("⏭️  Skipping sample data initialization")
        
        print("🎉 Databricks environment setup completed successfully!")
        
    except Exception as e:
        print(f"❌ Setup failed: {str(e)}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)
    finally:
        # Clean up Spark session if we created it
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
