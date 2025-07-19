#!/usr/bin/env python3
"""
Local test script for ENTSOE ETL pipeline with environment setup.
This script sets up test environment variables and runs basic functionality tests.
"""

import os
import sys
from datetime import datetime, timedelta

# Set up test environment variables
os.environ['ENTSOE_API_KEY'] = 'test_api_key_for_testing'
os.environ['DATABASE_URL'] = 'postgresql://test:test@localhost:5432/test_db'
os.environ['COUNTRY_EIC'] = '10Y1001A1001A82H'
os.environ['COUNTRY_CODE'] = 'DE'
os.environ['LOG_LEVEL'] = 'INFO'

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
        print(f"  - API Key: {settings.entsoe_api_key[:8]}...")
        print(f"  - Database URL: {settings.database_url.split('@')[1] if '@' in settings.database_url else 'Configured'}")
        
        return True
        
    except Exception as e:
        print(f"✗ Configuration test failed: {e}")
        return False

def test_country_config():
    """Test country configuration loading."""
    print("\n" + "=" * 50)
    print("TESTING COUNTRY CONFIGURATION")
    print("=" * 50)
    
    try:
        from config import get_country_info, get_all_countries
        
        # Test getting default country
        country_info = get_country_info()
        print(f"✓ Default country: {country_info['name']} ({country_info['code']})")
        print(f"  EIC Code: {country_info['eic']}")
        
        # Test getting all countries
        all_countries = get_all_countries()
        print(f"✓ Available countries: {len(all_countries)}")
        for code, info in list(all_countries.items())[:3]:  # Show first 3
            print(f"  - {code}: {info['name']}")
        
        return True
        
    except Exception as e:
        print(f"✗ Country configuration test failed: {e}")
        return False

def test_api_client_init():
    """Test API client initialization (without making actual API calls)."""
    print("\n" + "=" * 50)
    print("TESTING API CLIENT INITIALIZATION")
    print("=" * 50)
    
    try:
        from entsoe_api import ENTSOEAPIClient
        client = ENTSOEAPIClient()
        print("✓ API client initialized successfully")
        print(f"  - Base URL: {client.base_url}")
        print(f"  - Country EIC: {client.country_eic}")
        print(f"  - Country Code: {client.country_code}")
        
        return True
        
    except Exception as e:
        print(f"✗ API client initialization failed: {e}")
        return False

def test_transformer():
    """Test data transformer initialization."""
    print("\n" + "=" * 50)
    print("TESTING DATA TRANSFORMER")
    print("=" * 50)
    
    try:
        from transform import DataTransformer
        transformer = DataTransformer()
        print("✓ Data transformer initialized successfully")
        
        # Test with sample data
        import pandas as pd
        
        # Create sample balancing reserves data with correct schema
        sample_br_data = pd.DataFrame({
            'datetime_utc': [datetime.now()],
            'country_code': ['DE'],
            'reserve_type': ['FCR'],
            'amount_mw': [100.0],
            'price_eur': [50.0]
        })
        
        # Test transformation
        transformed_br = transformer.transform_balancing_reserves(sample_br_data)
        print(f"✓ Sample BR transformation: {len(transformed_br)} records")
        
        # Create sample day-ahead prices data with correct schema
        sample_dap_data = pd.DataFrame({
            'datetime_utc': [datetime.now()],
            'country_code': ['DE'],
            'price_eur_per_mwh': [50.0]
        })
        
        # Test transformation
        transformed_dap = transformer.transform_day_ahead_prices(sample_dap_data)
        print(f"✓ Sample DAP transformation: {len(transformed_dap)} records")
        
        return True
        
    except Exception as e:
        print(f"✗ Data transformer test failed: {e}")
        return False

def test_utils():
    """Test utility functions."""
    print("\n" + "=" * 50)
    print("TESTING UTILITY FUNCTIONS")
    print("=" * 50)
    
    try:
        from utils import setup_logging, get_date_range, parse_date_argument
        
        # Test logging setup
        logger = setup_logging("INFO")
        logger.info("Test log message")
        print("✓ Logging setup successful")
        
        # Test date range
        start_date_str = "2024-01-01"
        end_date_str = "2024-01-03"
        dates = get_date_range(start_date_str, end_date_str)
        print(f"✓ Date range generation: {len(dates)} dates")
        
        # Test date argument parsing
        start, end = parse_date_argument("daily")
        print(f"✓ Date argument parsing: {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}")
        
        return True
        
    except Exception as e:
        print(f"✗ Utility functions test failed: {e}")
        return False

def test_pipeline_init():
    """Test pipeline initialization (without database connection)."""
    print("\n" + "=" * 50)
    print("TESTING PIPELINE INITIALIZATION")
    print("=" * 50)
    
    try:
        from main import ENTSOEETLPipeline
        pipeline = ENTSOEETLPipeline()
        print("✓ Pipeline initialized successfully")
        print(f"  - API Client: {type(pipeline.api_client).__name__}")
        print(f"  - Transformer: {type(pipeline.transformer).__name__}")
        print(f"  - Loader: {type(pipeline.loader).__name__}")
        
        return True
        
    except Exception as e:
        print(f"✗ Pipeline initialization failed: {e}")
        return False

def main():
    """Run all tests."""
    print("ENTSOE ETL Pipeline - Local Test (No Database)")
    print("=" * 50)
    
    tests = [
        ("Imports", test_imports),
        ("Configuration", test_configuration),
        ("Country Config", test_country_config),
        ("API Client Init", test_api_client_init),
        ("Data Transformer", test_transformer),
        ("Utility Functions", test_utils),
        ("Pipeline Init", test_pipeline_init)
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
        print("\n🎉 ALL TESTS PASSED! Core functionality is working.")
        print("\nNext steps for full testing:")
        print("1. Set up a real ENTSOE API key")
        print("2. Set up a PostgreSQL database")
        print("3. Run the full pipeline test")
        return 0
    else:
        print(f"\n❌ {len(results) - passed} tests failed. Please fix the issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 