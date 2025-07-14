"""
Database loading module for ENTSOE ETL pipeline.
Handles PostgreSQL operations using psycopg2.
"""

import logging
import psycopg2
import pandas as pd
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from config import settings
from utils import validate_dataframe


class DatabaseLoader:
    """Handles database operations for the ETL pipeline."""
    
    def __init__(self):
        self.database_url = settings.database_url
        self.logger = logging.getLogger("entsoe_etl.load")
        self.engine = None
    
    def _get_engine(self):
        """Get SQLAlchemy engine for database operations."""
        if self.engine is None:
            try:
                self.engine = create_engine(self.database_url)
                self.logger.debug("Database engine created successfully")
            except Exception as e:
                self.logger.error(f"Failed to create database engine: {e}")
                raise
        return self.engine
    
    def test_connection(self) -> bool:
        """
        Test database connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.logger.info("Database connection test successful")
            return True
        except Exception as e:
            self.logger.error(f"Database connection test failed: {e}")
            return False
    
    def create_tables(self) -> bool:
        """
        Create database tables if they don't exist.
        
        Returns:
            True if tables created successfully, False otherwise
        """
        try:
            engine = self._get_engine()
            
            # SQL for creating tables
            balancing_reserves_sql = """
            CREATE TABLE IF NOT EXISTS balancing_reserves (
                id SERIAL PRIMARY KEY,
                country_code VARCHAR(10) NOT NULL,
                datetime_utc TIMESTAMP WITH TIME ZONE NOT NULL,
                product VARCHAR(100) NOT NULL,
                volume_mw DECIMAL(10, 2),
                price_eur_per_mw DECIMAL(10, 2),
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                UNIQUE(country_code, datetime_utc, product)
            );
            """
            
            day_ahead_prices_sql = """
            CREATE TABLE IF NOT EXISTS day_ahead_prices (
                id SERIAL PRIMARY KEY,
                country_code VARCHAR(10) NOT NULL,
                datetime_utc TIMESTAMP WITH TIME ZONE NOT NULL,
                price_eur_per_mwh DECIMAL(10, 2) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                UNIQUE(country_code, datetime_utc)
            );
            """
            
            # Create indexes for better performance
            balancing_reserves_indexes = """
            CREATE INDEX IF NOT EXISTS idx_balancing_reserves_datetime 
            ON balancing_reserves(datetime_utc);
            
            CREATE INDEX IF NOT EXISTS idx_balancing_reserves_country 
            ON balancing_reserves(country_code);
            
            CREATE INDEX IF NOT EXISTS idx_balancing_reserves_product 
            ON balancing_reserves(product);
            """
            
            day_ahead_prices_indexes = """
            CREATE INDEX IF NOT EXISTS idx_day_ahead_prices_datetime 
            ON day_ahead_prices(datetime_utc);
            
            CREATE INDEX IF NOT EXISTS idx_day_ahead_prices_country 
            ON day_ahead_prices(country_code);
            """
            
            with engine.connect() as conn:
                # Create tables
                conn.execute(text(balancing_reserves_sql))
                conn.execute(text(day_ahead_prices_sql))
                
                # Create indexes
                conn.execute(text(balancing_reserves_indexes))
                conn.execute(text(day_ahead_prices_indexes))
                
                conn.commit()
            
            self.logger.info("Database tables created successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create database tables: {e}")
            return False
    
    def load_balancing_reserves(self, df: pd.DataFrame) -> bool:
        """
        Load balancing reserves data into PostgreSQL.
        
        Args:
            df: DataFrame with balancing reserves data
        
        Returns:
            True if load successful, False otherwise
        """
        if df.empty:
            self.logger.warning("Empty balancing reserves DataFrame provided")
            return True
        
        # Validate DataFrame
        required_columns = ['country_code', 'datetime_utc', 'product', 'volume_mw', 'created_at']
        if not validate_dataframe(df, required_columns):
            self.logger.error("Invalid balancing reserves DataFrame")
            return False
        
        try:
            engine = self._get_engine()
            
            # Prepare data for insertion
            data_to_insert = []
            for _, row in df.iterrows():
                data_to_insert.append({
                    'country_code': row['country_code'],
                    'datetime_utc': row['datetime_utc'],
                    'product': row['product'],
                    'volume_mw': row['volume_mw'],
                    'price_eur_per_mw': row.get('price_eur_per_mw'),
                    'created_at': row['created_at']
                })
            
            # Insert data using pandas to_sql with conflict resolution
            df_to_insert = pd.DataFrame(data_to_insert)
            
            with engine.connect() as conn:
                # Use ON CONFLICT DO UPDATE for upsert behavior
                df_to_insert.to_sql(
                    'balancing_reserves_temp',
                    conn,
                    if_exists='replace',
                    index=False,
                    method='multi'
                )
                
                # Merge data using SQL
                merge_sql = """
                INSERT INTO balancing_reserves 
                (country_code, datetime_utc, product, volume_mw, price_eur_per_mw, created_at)
                SELECT country_code, datetime_utc, product, volume_mw, price_eur_per_mw, created_at
                FROM balancing_reserves_temp
                ON CONFLICT (country_code, datetime_utc, product)
                DO UPDATE SET
                    volume_mw = EXCLUDED.volume_mw,
                    price_eur_per_mw = EXCLUDED.price_eur_per_mw,
                    created_at = EXCLUDED.created_at;
                """
                
                conn.execute(text(merge_sql))
                
                # Clean up temp table
                conn.execute(text("DROP TABLE IF EXISTS balancing_reserves_temp"))
                
                conn.commit()
            
            self.logger.info(f"Successfully loaded {len(df)} balancing reserves records")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load balancing reserves data: {e}")
            return False
    
    def load_day_ahead_prices(self, df: pd.DataFrame) -> bool:
        """
        Load day-ahead prices data into PostgreSQL.
        
        Args:
            df: DataFrame with day-ahead prices data
        
        Returns:
            True if load successful, False otherwise
        """
        if df.empty:
            self.logger.warning("Empty day-ahead prices DataFrame provided")
            return True
        
        # Validate DataFrame
        required_columns = ['country_code', 'datetime_utc', 'price_eur_per_mwh', 'created_at']
        if not validate_dataframe(df, required_columns):
            self.logger.error("Invalid day-ahead prices DataFrame")
            return False
        
        try:
            engine = self._get_engine()
            
            # Prepare data for insertion
            data_to_insert = []
            for _, row in df.iterrows():
                data_to_insert.append({
                    'country_code': row['country_code'],
                    'datetime_utc': row['datetime_utc'],
                    'price_eur_per_mwh': row['price_eur_per_mwh'],
                    'created_at': row['created_at']
                })
            
            # Insert data using pandas to_sql with conflict resolution
            df_to_insert = pd.DataFrame(data_to_insert)
            
            with engine.connect() as conn:
                # Use ON CONFLICT DO UPDATE for upsert behavior
                df_to_insert.to_sql(
                    'day_ahead_prices_temp',
                    conn,
                    if_exists='replace',
                    index=False,
                    method='multi'
                )
                
                # Merge data using SQL
                merge_sql = """
                INSERT INTO day_ahead_prices 
                (country_code, datetime_utc, price_eur_per_mwh, created_at)
                SELECT country_code, datetime_utc, price_eur_per_mwh, created_at
                FROM day_ahead_prices_temp
                ON CONFLICT (country_code, datetime_utc)
                DO UPDATE SET
                    price_eur_per_mwh = EXCLUDED.price_eur_per_mwh,
                    created_at = EXCLUDED.created_at;
                """
                
                conn.execute(text(merge_sql))
                
                # Clean up temp table
                conn.execute(text("DROP TABLE IF EXISTS day_ahead_prices_temp"))
                
                conn.commit()
            
            self.logger.info(f"Successfully loaded {len(df)} day-ahead prices records")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load day-ahead prices data: {e}")
            return False
    
    def get_table_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the database tables.
        
        Returns:
            Dictionary with table statistics
        """
        try:
            engine = self._get_engine()
            
            stats = {}
            
            with engine.connect() as conn:
                # Get balancing reserves stats
                br_stats = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total_records,
                        MIN(datetime_utc) as earliest_date,
                        MAX(datetime_utc) as latest_date,
                        COUNT(DISTINCT product) as unique_products
                    FROM balancing_reserves
                """)).fetchone()
                
                stats['balancing_reserves'] = {
                    'total_records': br_stats[0],
                    'earliest_date': br_stats[1].isoformat() if br_stats[1] else None,
                    'latest_date': br_stats[2].isoformat() if br_stats[2] else None,
                    'unique_products': br_stats[3]
                }
                
                # Get day-ahead prices stats
                dap_stats = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total_records,
                        MIN(datetime_utc) as earliest_date,
                        MAX(datetime_utc) as latest_date,
                        AVG(price_eur_per_mwh) as avg_price
                    FROM day_ahead_prices
                """)).fetchone()
                
                stats['day_ahead_prices'] = {
                    'total_records': dap_stats[0],
                    'earliest_date': dap_stats[1].isoformat() if dap_stats[1] else None,
                    'latest_date': dap_stats[2].isoformat() if dap_stats[2] else None,
                    'avg_price': float(dap_stats[3]) if dap_stats[3] else None
                }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to get table stats: {e}")
            return {}
    
    def close_connection(self):
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            self.logger.debug("Database connection closed") 