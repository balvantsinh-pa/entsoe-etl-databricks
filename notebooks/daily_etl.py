# Databricks notebook source
# MAGIC %md
# MAGIC # ENTSOE Daily ETL Pipeline
# MAGIC 
# MAGIC This notebook runs the daily ETL pipeline for ENTSOE energy market data.
# MAGIC 
# MAGIC ## Features:
# MAGIC - Extract balancing reserves and day-ahead prices
# MAGIC - Transform and validate data
# MAGIC - Load data to PostgreSQL database
# MAGIC - Comprehensive logging and error handling

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: Environment Setup

# COMMAND ----------

import sys
import os
from datetime import datetime, timedelta

# Add the repository path to Python path
repo_path = "/Workspace/Repos/your-repo-name"  # Update with your actual repo path
sys.path.append(repo_path)

print(f"Repository path: {repo_path}")
print(f"Python path: {sys.path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: Import and Initialize Pipeline

# COMMAND ----------

try:
    from main import ENTSOEETLPipeline
    from src.config import settings
    
    print("✓ Successfully imported ETL pipeline")
    print(f"API Base URL: {settings.entsoe_base_url}")
    print(f"Country Code: {settings.country_code}")
    print(f"Is Databricks: {settings.is_databricks}")
    
except Exception as e:
    print(f"✗ Import failed: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: Test Configuration

# COMMAND ----------

print("=" * 60)
print("CONFIGURATION TEST")
print("=" * 60)

# Test API key
if settings.entsoe_api_key:
    print(f"✓ ENTSOE API Key: {settings.entsoe_api_key[:8]}...")
else:
    print("✗ ENTSOE API Key not found")

# Test database URL
if settings.database_url:
    print(f"✓ Database URL: {settings.database_url.split('@')[1] if '@' in settings.database_url else 'Configured'}")
else:
    print("✗ Database URL not found")

# Test country configuration
try:
    from src.config import get_country_info
    country_info = get_country_info()
    print(f"✓ Country: {country_info['name']} ({country_info['code']})")
    print(f"  EIC Code: {country_info['eic']}")
except Exception as e:
    print(f"✗ Country configuration error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: Initialize Pipeline

# COMMAND ----------

try:
    pipeline = ENTSOEETLPipeline()
    print("✓ Pipeline initialized successfully")
except Exception as e:
    print(f"✗ Pipeline initialization failed: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: Test Database Connection

# COMMAND ----------

print("=" * 60)
print("DATABASE CONNECTION TEST")
print("=" * 60)

try:
    if pipeline.loader.test_connection():
        print("✓ Database connection successful")
        
        # Test table creation
        if pipeline.loader.create_tables():
            print("✓ Database tables created/verified")
        else:
            print("✗ Database table creation failed")
    else:
        print("✗ Database connection failed")
        
except Exception as e:
    print(f"✗ Database test failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: Test API Connection

# COMMAND ----------

print("=" * 60)
print("API CONNECTION TEST")
print("=" * 60)

try:
    # Test with a recent date
    test_date = datetime.now() - timedelta(days=2)
    
    print(f"Testing API with date: {test_date.strftime('%Y-%m-%d')}")
    
    # Test balancing reserves
    br_df = pipeline.api_client.get_balancing_reserves(test_date)
    print(f"✓ Balancing reserves: {len(br_df)} records")
    
    # Test day-ahead prices
    dap_df = pipeline.api_client.get_day_ahead_prices(test_date)
    print(f"✓ Day-ahead prices: {len(dap_df)} records")
    
    if not br_df.empty:
        print(f"Sample BR data:\n{br_df.head(2)}")
    
    if not dap_df.empty:
        print(f"Sample DAP data:\n{dap_df.head(2)}")
        
except Exception as e:
    print(f"✗ API test failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: Run Daily ETL (Test Mode)

# COMMAND ----------

print("=" * 60)
print("DAILY ETL TEST RUN")
print("=" * 60)

# Test with yesterday's data
test_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
print(f"Testing ETL for date: {test_date}")

try:
    success = pipeline.run_daily_etl(test_date)
    
    if success:
        print("✅ Daily ETL completed successfully!")
        
        # Show database stats
        stats = pipeline.loader.get_table_stats()
        print("\nDatabase Statistics:")
        for table, count in stats.items():
            print(f"  - {table}: {count} records")
    else:
        print("❌ Daily ETL failed!")
        
except Exception as e:
    print(f"❌ ETL test failed: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: Data Validation

# COMMAND ----------

print("=" * 60)
print("DATA VALIDATION")
print("=" * 60)

try:
    # Query recent data
    import pandas as pd
    
    # Get balancing reserves for the test date
    br_query = f"""
    SELECT * FROM balancing_reserves 
    WHERE date = '{test_date}' 
    ORDER BY timestamp DESC 
    LIMIT 5
    """
    
    br_data = pd.read_sql(br_query, pipeline.loader.connection)
    print(f"Balancing reserves for {test_date}: {len(br_data)} records")
    if not br_data.empty:
        print(br_data.head())
    
    # Get day-ahead prices for the test date
    dap_query = f"""
    SELECT * FROM day_ahead_prices 
    WHERE date = '{test_date}' 
    ORDER BY timestamp DESC 
    LIMIT 5
    """
    
    dap_data = pd.read_sql(dap_query, pipeline.loader.connection)
    print(f"\nDay-ahead prices for {test_date}: {len(dap_data)} records")
    if not dap_data.empty:
        print(dap_data.head())
        
except Exception as e:
    print(f"✗ Data validation failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9: Cleanup

# COMMAND ----------

print("=" * 60)
print("CLEANUP")
print("=" * 60)

try:
    pipeline.cleanup()
    print("✓ Pipeline cleanup completed")
except Exception as e:
    print(f"✗ Cleanup failed: {e}")

print("\n" + "=" * 60)
print("🎉 END-TO-END PIPELINE TEST COMPLETED!")
print("=" * 60) 