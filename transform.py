"""
Data transformation module for ENTSOE ETL pipeline.
Handles UTC time normalization, DST conversion, and data cleaning.
"""

import logging
import pandas as pd
from datetime import datetime, timezone
from typing import Optional, List
import pytz

from utils import normalize_utc_time, safe_float, validate_dataframe


class DataTransformer:
    """Handles data transformation and cleaning operations."""
    
    def __init__(self):
        self.logger = logging.getLogger("entsoe_etl.transform")
    
    def transform_balancing_reserves(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform and clean balancing reserves data.
        
        Args:
            df: Raw balancing reserves DataFrame
        
        Returns:
            Cleaned and transformed DataFrame
        """
        if df.empty:
            self.logger.warning("Empty balancing reserves DataFrame provided")
            return df
        
        self.logger.info(f"Transforming {len(df)} balancing reserves records")
        
        try:
            # Create a copy to avoid modifying original
            df_transformed = df.copy()
            
            # Ensure datetime_utc is timezone-aware and in UTC
            df_transformed['datetime_utc'] = df_transformed['datetime_utc'].apply(
                lambda x: normalize_utc_time(x) if pd.notna(x) else x
            )
            
            # Clean and validate numeric columns
            df_transformed['volume_mw'] = df_transformed['volume_mw'].apply(safe_float)
            
            # Remove rows with invalid data
            df_transformed = self._remove_invalid_records(df_transformed, 'balancing_reserves')
            
            # Add created_at timestamp
            df_transformed['created_at'] = datetime.now(timezone.utc)
            
            # Sort by datetime
            df_transformed = df_transformed.sort_values('datetime_utc').reset_index(drop=True)
            
            self.logger.info(f"Transformed {len(df_transformed)} valid balancing reserves records")
            return df_transformed
            
        except Exception as e:
            self.logger.error(f"Failed to transform balancing reserves data: {e}")
            raise
    
    def transform_day_ahead_prices(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform and clean day-ahead prices data.
        
        Args:
            df: Raw day-ahead prices DataFrame
        
        Returns:
            Cleaned and transformed DataFrame
        """
        if df.empty:
            self.logger.warning("Empty day-ahead prices DataFrame provided")
            return df
        
        self.logger.info(f"Transforming {len(df)} day-ahead prices records")
        
        try:
            # Create a copy to avoid modifying original
            df_transformed = df.copy()
            
            # Ensure datetime_utc is timezone-aware and in UTC
            df_transformed['datetime_utc'] = df_transformed['datetime_utc'].apply(
                lambda x: normalize_utc_time(x) if pd.notna(x) else x
            )
            
            # Clean and validate numeric columns
            df_transformed['price_eur_per_mwh'] = df_transformed['price_eur_per_mwh'].apply(safe_float)
            
            # Remove rows with invalid data
            df_transformed = self._remove_invalid_records(df_transformed, 'day_ahead_prices')
            
            # Add created_at timestamp
            df_transformed['created_at'] = datetime.now(timezone.utc)
            
            # Sort by datetime
            df_transformed = df_transformed.sort_values('datetime_utc').reset_index(drop=True)
            
            self.logger.info(f"Transformed {len(df_transformed)} valid day-ahead prices records")
            return df_transformed
            
        except Exception as e:
            self.logger.error(f"Failed to transform day-ahead prices data: {e}")
            raise
    
    def _remove_invalid_records(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """
        Remove records with invalid or missing data.
        
        Args:
            df: DataFrame to clean
            data_type: Type of data for logging purposes
        
        Returns:
            Cleaned DataFrame
        """
        initial_count = len(df)
        
        # Remove rows with missing datetime
        df_clean = df.dropna(subset=['datetime_utc'])
        
        # Remove rows with missing country_code
        df_clean = df_clean.dropna(subset=['country_code'])
        
        # Data type specific cleaning
        if data_type == 'balancing_reserves':
            # Remove rows with missing product
            df_clean = df_clean.dropna(subset=['product'])
            
            # Remove rows with invalid volume (negative or zero)
            df_clean = df_clean[
                (df_clean['volume_mw'].notna()) & 
                (df_clean['volume_mw'] > 0)
            ]
        
        elif data_type == 'day_ahead_prices':
            # Remove rows with invalid price (negative)
            df_clean = df_clean[
                (df_clean['price_eur_per_mwh'].notna()) & 
                (df_clean['price_eur_per_mwh'] >= 0)
            ]
        
        # Remove duplicates based on key columns
        if data_type == 'balancing_reserves':
            df_clean = df_clean.drop_duplicates(
                subset=['country_code', 'datetime_utc', 'product'],
                keep='last'
            )
        elif data_type == 'day_ahead_prices':
            df_clean = df_clean.drop_duplicates(
                subset=['country_code', 'datetime_utc'],
                keep='last'
            )
        
        removed_count = initial_count - len(df_clean)
        if removed_count > 0:
            self.logger.info(f"Removed {removed_count} invalid {data_type} records")
        
        return df_clean
    
    def validate_transformed_data(self, df: pd.DataFrame, data_type: str) -> bool:
        """
        Validate transformed data meets requirements.
        
        Args:
            df: Transformed DataFrame to validate
            data_type: Type of data being validated
        
        Returns:
            True if valid, False otherwise
        """
        if df.empty:
            self.logger.warning(f"Empty {data_type} DataFrame")
            return False
        
        # Check required columns
        if data_type == 'balancing_reserves':
            required_columns = ['country_code', 'datetime_utc', 'product', 'volume_mw', 'created_at']
        elif data_type == 'day_ahead_prices':
            required_columns = ['country_code', 'datetime_utc', 'price_eur_per_mwh', 'created_at']
        else:
            self.logger.error(f"Unknown data type: {data_type}")
            return False
        
        if not validate_dataframe(df, required_columns):
            self.logger.error(f"Missing required columns for {data_type}")
            return False
        
        # Check data types
        if not pd.api.types.is_datetime64_any_dtype(df['datetime_utc']):
            self.logger.error(f"datetime_utc column is not datetime type for {data_type}")
            return False
        
        # Check for timezone awareness
        if df['datetime_utc'].dt.tz is None:
            self.logger.error(f"datetime_utc column is not timezone-aware for {data_type}")
            return False
        
        # Check for UTC timezone
        if not all(dt.tzinfo == timezone.utc for dt in df['datetime_utc'] if pd.notna(dt)):
            self.logger.error(f"datetime_utc column is not in UTC for {data_type}")
            return False
        
        self.logger.info(f"Validation passed for {data_type} data")
        return True
    
    def get_data_summary(self, df: pd.DataFrame, data_type: str) -> dict:
        """
        Generate summary statistics for transformed data.
        
        Args:
            df: Transformed DataFrame
            data_type: Type of data for summary
        
        Returns:
            Dictionary with summary statistics
        """
        if df.empty:
            return {
                'data_type': data_type,
                'record_count': 0,
                'date_range': None,
                'summary_stats': {}
            }
        
        summary = {
            'data_type': data_type,
            'record_count': len(df),
            'date_range': {
                'start': df['datetime_utc'].min().isoformat(),
                'end': df['datetime_utc'].max().isoformat()
            }
        }
        
        # Data type specific statistics
        if data_type == 'balancing_reserves':
            summary['summary_stats'] = {
                'unique_products': df['product'].nunique(),
                'volume_stats': {
                    'mean': df['volume_mw'].mean(),
                    'min': df['volume_mw'].min(),
                    'max': df['volume_mw'].max()
                }
            }
        elif data_type == 'day_ahead_prices':
            summary['summary_stats'] = {
                'price_stats': {
                    'mean': df['price_eur_per_mwh'].mean(),
                    'min': df['price_eur_per_mwh'].min(),
                    'max': df['price_eur_per_mwh'].max()
                }
            }
        
        return summary 