# Databricks notebook source
# MAGIC %md
# MAGIC # ENTSOE Historical ETL Pipeline
# MAGIC 
# MAGIC This notebook runs the historical ETL pipeline for ENTSOE energy market data.
# MAGIC 
# MAGIC ## Features:
# MAGIC - Extract historical balancing reserves and day-ahead prices
# MAGIC - Process data in configurable date ranges
# MAGIC - Transform and validate data
# MAGIC - Load data to PostgreSQL database
# MAGIC - Progress tracking and error handling

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
    print(f"Default Start Date: {settings.default_start_date}")
    
except Exception as e:
    print(f"✗ Import failed: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: Define Historical ETL Parameters

# COMMAND ----------

print("=" * 60)
print("HISTORICAL ETL PARAMETERS")
print("=" * 60)

# Define date range for testing (last 7 days)
end_date = datetime.now() - timedelta(days=1)  # Yesterday
start_date = end_date - timedelta(days=6)      # 7 days ago

print(f"Start Date: {start_date.strftime('%Y-%m-%d')}")
print(f"End Date: {end_date.strftime('%Y-%m-%d')}")
print(f"Total Days: {(end_date - start_date).days + 1}")

# You can modify these dates for different test ranges
# start_date = datetime(2024, 1, 1)
# end_date = datetime(2024, 1, 31)

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
# MAGIC ## Cell 5: Pre-ETL Database Check

# COMMAND ----------

print("=" * 60)
print("PRE-ETL DATABASE CHECK")
print("=" * 60)

try:
    # Check existing data
    import pandas as pd
    
    # Count existing records
    br_count_query = "SELECT COUNT(*) as count FROM balancing_reserves"
    dap_count_query = "SELECT COUNT(*) as count FROM day_ahead_prices"
    
    br_count = pd.read_sql(br_count_query, pipeline.loader.connection).iloc[0]['count']
    dap_count = pd.read_sql(dap_count_query, pipeline.loader.connection).iloc[0]['count']
    
    print(f"Existing balancing reserves: {br_count} records")
    print(f"Existing day-ahead prices: {dap_count} records")
    
    # Check date range of existing data
    br_date_query = """
    SELECT MIN(date) as min_date, MAX(date) as max_date 
    FROM balancing_reserves
    """
    
    br_dates = pd.read_sql(br_date_query, pipeline.loader.connection)
    if not br_dates.empty and br_dates.iloc[0]['min_date']:
        print(f"BR date range: {br_dates.iloc[0]['min_date']} to {br_dates.iloc[0]['max_date']}")
    
except Exception as e:
    print(f"✗ Pre-ETL check failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: Run Historical ETL

# COMMAND ----------

print("=" * 60)
print("RUNNING HISTORICAL ETL")
print("=" * 60)

start_time = datetime.now()
print(f"ETL started at: {start_time}")

try:
    success = pipeline.run_historical_etl(
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d')
    )
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    if success:
        print(f"✅ Historical ETL completed successfully!")
        print(f"Duration: {duration}")
        
        # Show final database stats
        stats = pipeline.loader.get_table_stats()
        print("\nFinal Database Statistics:")
        for table, count in stats.items():
            print(f"  - {table}: {count} records")
    else:
        print(f"❌ Historical ETL failed!")
        print(f"Duration: {duration}")
        
except Exception as e:
    print(f"❌ Historical ETL failed: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: Post-ETL Data Validation

# COMMAND ----------

print("=" * 60)
print("POST-ETL DATA VALIDATION")
print("=" * 60)

try:
    # Check new data in the date range
    br_new_query = f"""
    SELECT COUNT(*) as count 
    FROM balancing_reserves 
    WHERE date >= '{start_date.strftime('%Y-%m-%d')}' 
    AND date <= '{end_date.strftime('%Y-%m-%d')}'
    """
    
    dap_new_query = f"""
    SELECT COUNT(*) as count 
    FROM day_ahead_prices 
    WHERE date >= '{start_date.strftime('%Y-%m-%d')}' 
    AND date <= '{end_date.strftime('%Y-%m-%d')}'
    """
    
    br_new_count = pd.read_sql(br_new_query, pipeline.loader.connection).iloc[0]['count']
    dap_new_count = pd.read_sql(dap_new_query, pipeline.loader.connection).iloc[0]['count']
    
    print(f"New balancing reserves in range: {br_new_count} records")
    print(f"New day-ahead prices in range: {dap_new_count} records")
    
    # Show sample data
    if br_new_count > 0:
        br_sample_query = f"""
        SELECT * FROM balancing_reserves 
        WHERE date >= '{start_date.strftime('%Y-%m-%d')}' 
        AND date <= '{end_date.strftime('%Y-%m-%d')}'
        ORDER BY timestamp DESC 
        LIMIT 3
        """
        br_sample = pd.read_sql(br_sample_query, pipeline.loader.connection)
        print(f"\nSample BR data:\n{br_sample}")
    
    if dap_new_count > 0:
        dap_sample_query = f"""
        SELECT * FROM day_ahead_prices 
        WHERE date >= '{start_date.strftime('%Y-%m-%d')}' 
        AND date <= '{end_date.strftime('%Y-%m-%d')}'
        ORDER BY timestamp DESC 
        LIMIT 3
        """
        dap_sample = pd.read_sql(dap_sample_query, pipeline.loader.connection)
        print(f"\nSample DAP data:\n{dap_sample}")
        
except Exception as e:
    print(f"✗ Post-ETL validation failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: Data Quality Analysis

# COMMAND ----------

print("=" * 60)
print("DATA QUALITY ANALYSIS")
print("=" * 60)

try:
    # Check for missing data
    missing_dates_query = f"""
    WITH date_series AS (
        SELECT generate_series(
            '{start_date.strftime('%Y-%m-%d')}'::date,
            '{end_date.strftime('%Y-%m-%d')}'::date,
            '1 day'::interval
        )::date as date
    )
    SELECT ds.date, 
           CASE WHEN br.date IS NULL THEN 'Missing' ELSE 'Present' END as br_status,
           CASE WHEN dap.date IS NULL THEN 'Missing' ELSE 'Present' END as dap_status
    FROM date_series ds
    LEFT JOIN (SELECT DISTINCT date FROM balancing_reserves) br ON ds.date = br.date
    LEFT JOIN (SELECT DISTINCT date FROM day_ahead_prices) dap ON ds.date = dap.date
    ORDER BY ds.date
    """
    
    missing_data = pd.read_sql(missing_dates_query, pipeline.loader.connection)
    
    print("Data availability by date:")
    print(missing_data)
    
    # Summary
    missing_br = len(missing_data[missing_data['br_status'] == 'Missing'])
    missing_dap = len(missing_data[missing_data['dap_status'] == 'Missing'])
    
    print(f"\nSummary:")
    print(f"  - Missing BR data: {missing_br} days")
    print(f"  - Missing DAP data: {missing_dap} days")
    print(f"  - Total days processed: {len(missing_data)} days")
    
except Exception as e:
    print(f"✗ Data quality analysis failed: {e}")

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
print("🎉 HISTORICAL ETL PIPELINE TEST COMPLETED!")
print("=" * 60) 