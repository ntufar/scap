#!/usr/bin/env python3
"""
Entry point for dbx: Simulate sample data for Navigator Supply Chain Lakehouse.
"""

import sys
import os
import argparse
from pyspark.sql import SparkSession

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.cli import simulate_data


def main():
    """Main entry point for dbx deployment."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Simulate sample data")
    parser.add_argument("--domain", required=True, choices=["supplier", "shipment", "inventory"],
                       help="Data domain to simulate")
    parser.add_argument("--output-path", required=True, help="Output path for simulated data")
    parser.add_argument("--record-count", type=int, default=1000, help="Number of records to generate")
    
    args = parser.parse_args()
    
    print(f"Simulating {args.record_count} {args.domain} records...")
    
    try:
        # Call the CLI function
        simulate_data(args)
        print(f"✅ Data simulation completed successfully!")
        
    except Exception as e:
        print(f"❌ Simulation failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
