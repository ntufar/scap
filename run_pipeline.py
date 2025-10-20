#!/usr/bin/env python3
"""
Entry point for dbx: Run data pipeline for Navigator Supply Chain Lakehouse.
"""

import sys
import os
import argparse
from pyspark.sql import SparkSession

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.cli import run_pipeline


def main():
    """Main entry point for dbx deployment."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run data pipeline")
    parser.add_argument("--stage", required=True, choices=["bronze", "silver", "gold", "all"],
                       help="Pipeline stage to run")
    parser.add_argument("--domain", choices=["supplier", "shipment", "inventory", "all"],
                       default="all", help="Data domain to process")
    
    args = parser.parse_args()
    
    print(f"Running pipeline stage: {args.stage} for domain: {args.domain}")
    
    try:
        # Call the CLI function
        run_pipeline(args)
        print(f"✅ Pipeline execution completed successfully!")
        
    except Exception as e:
        print(f"❌ Pipeline execution failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
