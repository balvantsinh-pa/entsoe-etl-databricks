"""
Databricks initialization script for ENTSOE ETL pipeline.
Run this script to initialize and test the Databricks environment.
"""

import os
import sys
import logging
from datetime import datetime

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def setup_databricks_environment():
    """Set up Databricks environment and test configuration."""
    
    print("=" * 60)
    print("ENTSOE ETL Pipeline - Databricks Environment Setup")
    print("=" * 60)
    
    # Check if running in Databricks
    databricks_env = any([
        'DATABRICKS_RUNTIME_VERSION' in os.environ,
        'SPARK_HOME' in os.environ,
        'DATABRICKS_WORKSPACE_URL' in os.environ
    ])
    
    if databricks_env:
        print("✓ Running in Databricks environment")
        print(f"  - Runtime Version: {os.environ.get('DATABRICKS_RUNTIME_VERSION', 'Unknown')}")
        print(f"  - Workspace URL: {os.environ.get('DATABRICKS_WORKSPACE_URL', 'Unknown')}")
        print(f"  - Cluster ID: {os.environ.get('DATABRICKS_CLUSTER_ID', 'Unknown')}")
    else:
        print("⚠ Running in local environment (not Databricks)")
    
    # Test imports
    print("\nTesting imports...")
    try:
        import requests
        print("✓ requests imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import requests: {e}")
        return False
    
    try:
        import pandas as pd
        print("✓ pandas imported successfully")
        print(f"  - Version: {pd.__version__}")
    except ImportError as e:
        print(f"✗ Failed to import pandas: {e}")
        return False
    
    try:
        import psycopg2
        print("✓ psycopg2 imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import psycopg2: {e}")
        return False
    
    try:
        import xmltodict
        print("✓ xmltodict imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import xmltodict: {e}")
        return False
    
    # Test configuration
    print("\nTesting configuration...")
    try:
        from config import settings
        print("✓ Configuration loaded successfully")
        print(f"  - API Base URL: {settings.entsoe_base_url}")
        print(f"  - Country EIC: {settings.country_eic}")
        print(f"  - Country Code: {settings.country_code}")
        print(f"  - Log Level: {settings.log_level}")
        print(f"  - Is Databricks: {settings.is_databricks}")
    except Exception as e:
        print(f"✗ Configuration test failed: {e}")
        return False
    
    # Test logging
    print("\nTesting logging...")
    try:
        from utils import setup_logging
        logger = setup_logging("INFO")
        logger.info("Logging test successful")
        print("✓ Logging setup successful")
    except Exception as e:
        print(f"✗ Logging test failed: {e}")
        return False
    
    # Test API client
    print("\nTesting API client...")
    try:
        from entsoe_api import ENTSOEAPIClient
        client = ENTSOEAPIClient()
        print("✓ API client initialized successfully")
    except Exception as e:
        print(f"✗ API client test failed: {e}")
        return False
    
    # Test database connection
    print("\nTesting database connection...")
    try:
        from load import DatabaseLoader
        loader = DatabaseLoader()
        if loader.test_connection():
            print("✓ Database connection successful")
        else:
            print("✗ Database connection failed")
            return False
    except Exception as e:
        print(f"✗ Database connection test failed: {e}")
        return False
    
    # Test file paths
    print("\nTesting file paths...")
    try:
        from utils import get_databricks_file_path
        test_path = get_databricks_file_path("main.py")
        print(f"✓ File path resolution: {test_path}")
    except Exception as e:
        print(f"✗ File path test failed: {e}")
        return False
    
    print("\n" + "=" * 60)
    print("✓ All tests passed! Environment is ready for ETL pipeline.")
    print("=" * 60)
    
    return True


def show_usage_examples():
    """Show usage examples for the ETL pipeline."""
    
    print("\n" + "=" * 60)
    print("USAGE EXAMPLES")
    print("=" * 60)
    
    print("\n1. Run daily ETL (yesterday's data):")
    print("   python main.py --mode daily")
    
    print("\n2. Run daily ETL for specific date:")
    print("   python main.py --mode daily --date 2024-01-15")
    
    print("\n3. Run historical ETL (default: 2024-01-01 to today):")
    print("   python main.py --mode historical")
    
    print("\n4. Run historical ETL for specific date range:")
    print("   python main.py --mode historical --start-date 2024-01-01 --end-date 2024-01-31")
    
    print("\n5. In Databricks notebook:")
    print("   %run /Workspace/Repos/your-repo/main.py --mode daily")
    
    print("\n" + "=" * 60)


def main():
    """Main function for Databricks initialization."""
    
    success = setup_databricks_environment()
    
    if success:
        show_usage_examples()
        print(f"\nInitialization completed at: {datetime.now()}")
        return 0
    else:
        print(f"\nInitialization failed at: {datetime.now()}")
        print("Please check the error messages above and fix any issues.")
        return 1


if __name__ == "__main__":
    sys.exit(main()) 