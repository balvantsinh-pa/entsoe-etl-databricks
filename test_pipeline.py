#!/usr/bin/env python3
"""
Quick test script for ENTSOE ETL pipeline in Databricks.
Run this to validate all components before running full ETL.
"""

import sys
import os
from datetime import datetime, timedelta

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def test_imports():
    """Test all required imports."""
    print("=" * 50)
    print("TESTING IMPORTS")
    print("=" * 50)
    
    try:
        import requests
        print("✓ requests")
    except ImportError as e:
        print(f"✗ requests: {e}")
        return False
    
    try:
        import pandas as pd
        print(f"✓ pandas ({pd.__version__})")
    except ImportError as e:
        print(f"✗ pandas: {e}")
        return False
    
    try:
        import psycopg2
        print("✓ psycopg2")
    except ImportError as e:
        print(f"✗ psycopg2: {e}")
        return False
    
    try:
        import xmltodict
        print("✓ xmltodict")
    except ImportError as e:
        print(f"✗ xmltodict: {e}")
        return False
    
    return True

def test_configuration():
    """Test configuration loading."""
    print("\n" + "=" * 50)
    print("TESTING CONFIGURATION")
    print("=" * 50)
    
    try:
        from config import settings
        print(f"✓ Configuration loaded")
        print(f"  - API Base URL: {settings.entsoe_base_url}")
        print(f"  - Country Code: {settings.country_code}")
        print(f"  - Is Databricks: {settings.is_databricks}")
        
        # Check required settings
        if not settings.entsoe_api_key:
            print("✗ ENTSOE API key not found")
            return False
        
        if not settings.database_url:
            print("✗ Database URL not found")
            return False
        
        print("✓ All required settings present")
        return True
        
    except Exception as e:
        print(f"✗ Configuration test failed: {e}")
        return False

def test_api_client():
    """Test API client initialization."""
    print("\n" + "=" * 50)
    print("TESTING API CLIENT")
    print("=" * 50)
    
    try:
        from entsoe_api import ENTSOEAPIClient
        client = ENTSOEAPIClient()
        print("✓ API client initialized")
        
        # Test with a recent date
        test_date = datetime.now() - timedelta(days=2)
        print(f"Testing API call for {test_date.strftime('%Y-%m-%d')}")
        
        # Test balancing reserves
        br_df = client.get_balancing_reserves(test_date)
        print(f"✓ Balancing reserves: {len(br_df)} records")
        
        # Test day-ahead prices
        dap_df = client.get_day_ahead_prices(test_date)
        print(f"✓ Day-ahead prices: {len(dap_df)} records")
        
        return True
        
    except Exception as e:
        print(f"✗ API client test failed: {e}")
        return False

def test_database():
    """Test database connection."""
    print("\n" + "=" * 50)
    print("TESTING DATABASE")
    print("=" * 50)
    
    try:
        from postgres_writer import PostgresWriter
        loader = PostgresWriter()
        
        if loader.test_connection():
            print("✓ Database connection successful")
            
            if loader.create_tables():
                print("✓ Database tables created/verified")
                return True
            else:
                print("✗ Database table creation failed")
                return False
        else:
            print("✗ Database connection failed")
            return False
            
    except Exception as e:
        print(f"✗ Database test failed: {e}")
        return False

def test_pipeline():
    """Test full pipeline initialization."""
    print("\n" + "=" * 50)
    print("TESTING PIPELINE")
    print("=" * 50)
    
    try:
        from main import ENTSOEETLPipeline
        pipeline = ENTSOEETLPipeline()
        print("✓ Pipeline initialized successfully")
        
        # Test cleanup
        pipeline.cleanup()
        print("✓ Pipeline cleanup successful")
        
        return True
        
    except Exception as e:
        print(f"✗ Pipeline test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("ENTSOE ETL Pipeline - Quick Test")
    print("=" * 50)
    
    tests = [
        ("Imports", test_imports),
        ("Configuration", test_configuration),
        ("API Client", test_api_client),
        ("Database", test_database),
        ("Pipeline", test_pipeline)
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"✗ {test_name} test crashed: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("TEST SUMMARY")
    print("=" * 50)
    
    passed = 0
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nPassed: {passed}/{len(results)} tests")
    
    if passed == len(results):
        print("\n🎉 ALL TESTS PASSED! Pipeline is ready to run.")
        print("\nNext steps:")
        print("1. Run daily ETL: python main.py --mode daily")
        print("2. Run historical ETL: python main.py --mode historical --start-date 2024-01-01")
        print("3. Or use the Databricks notebooks in the notebooks/ directory")
        return 0
    else:
        print(f"\n❌ {len(results) - passed} tests failed. Please fix the issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 