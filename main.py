"""
Main ETL orchestrator for ENTSOE data pipeline.
Coordinates extraction, transformation, and loading of energy market data.
Updated for new src/ structure and Databricks-friendly ETL pipeline.
"""

import argparse
import logging
import sys
import os
from datetime import datetime, timezone
from typing import List, Optional

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from config import settings, get_country_info, get_databricks_info
from utils import setup_logging, get_date_range, parse_date_argument
from entsoe_api import ENTSOEAPIClient
from transform import DataTransformer
from postgres_writer import PostgresWriter


class ENTSOEETLPipeline:
    """Main ETL pipeline orchestrator."""
    
    def __init__(self, country_code: str = None):
        self.logger = setup_logging(settings.log_level)
        self.country_info = get_country_info(country_code)
        self.api_client = ENTSOEAPIClient(country_code=self.country_info['code'])
        self.transformer = DataTransformer()
        self.loader = PostgresWriter()
        
        # Log Databricks environment info
        if settings.is_databricks:
            self.logger.info("Running in Databricks environment")
            databricks_info = get_databricks_info()
            self.logger.info(f"Databricks info: {databricks_info}")
    
    def run_historical_etl(self, start_date: str = None, end_date: Optional[str] = None) -> bool:
        """
        Run ETL pipeline for historical data.
        
        Args:
            start_date: Start date in YYYY-MM-DD format (defaults to config default)
            end_date: End date in YYYY-MM-DD format (defaults to today)
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Set default start date if not provided
            if start_date is None:
                start_date = settings.default_start_date
            
            # Set end date to today if not provided
            if end_date is None:
                end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            
            self.logger.info(f"Starting historical ETL from {start_date} to {end_date}")
            self.logger.info(f"Target country: {self.country_info['name']} ({self.country_info['code']})")
            
            # Get date range
            dates = get_date_range(start_date, end_date)
            self.logger.info(f"Processing {len(dates)} days")
            
            # Initialize database
            if not self._initialize_database():
                return False
            
            # Process each date
            success_count = 0
            error_count = 0
            
            for date in dates:
                try:
                    self.logger.info(f"Processing date: {date.strftime('%Y-%m-%d')}")
                    
                    if self._process_single_date(date):
                        success_count += 1
                    else:
                        error_count += 1
                        
                except Exception as e:
                    self.logger.error(f"Error processing date {date.strftime('%Y-%m-%d')}: {e}")
                    error_count += 1
            
            # Log final statistics
            self.logger.info(f"Historical ETL completed. Success: {success_count}, Errors: {error_count}")
            
            # Get database statistics
            self._log_database_stats()
            
            return error_count == 0
            
        except Exception as e:
            self.logger.error(f"Historical ETL failed: {e}")
            return False
    
    def run_daily_etl(self, date_arg: str = "daily") -> bool:
        """
        Run ETL pipeline for daily data.
        
        Args:
            date_arg: Date string or "daily" for yesterday
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Parse date argument
            start_date, end_date = parse_date_argument(date_arg)
            
            self.logger.info(f"Starting daily ETL for {start_date.strftime('%Y-%m-%d')}")
            self.logger.info(f"Target country: {self.country_info['name']} ({self.country_info['code']})")
            
            # Initialize database
            if not self._initialize_database():
                return False
            
            # Process the date
            success = self._process_single_date(start_date)
            
            if success:
                self.logger.info("Daily ETL completed successfully")
                self._log_database_stats()
            else:
                self.logger.error("Daily ETL failed")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Daily ETL failed: {e}")
            return False
    
    def _initialize_database(self) -> bool:
        """
        Initialize database connection and create tables.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Test database connection
            if not self.loader.test_connection():
                self.logger.error("Database connection test failed")
                return False
            
            # Create tables if they don't exist
            if not self.loader.create_tables():
                self.logger.error("Failed to create database tables")
                return False
            
            self.logger.info("Database initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Database initialization failed: {e}")
            return False
    
    def _process_single_date(self, date: datetime) -> bool:
        """
        Process ETL for a single date.
        
        Args:
            date: Date to process
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Extract data
            balancing_reserves_df = self.api_client.get_balancing_reserves(date)
            day_ahead_prices_df = self.api_client.get_day_ahead_prices(date)
            
            # Transform data
            if not balancing_reserves_df.empty:
                balancing_reserves_df = self.transformer.transform_balancing_reserves(balancing_reserves_df)
                if not self.transformer.validate_transformed_data(balancing_reserves_df, 'balancing_reserves'):
                    self.logger.error("Balancing reserves data validation failed")
                    return False
            
            if not day_ahead_prices_df.empty:
                day_ahead_prices_df = self.transformer.transform_day_ahead_prices(day_ahead_prices_df)
                if not self.transformer.validate_transformed_data(day_ahead_prices_df, 'day_ahead_prices'):
                    self.logger.error("Day-ahead prices data validation failed")
                    return False
            
            # Load data
            br_success = self.loader.write_balancing_reserves(balancing_reserves_df)
            dap_success = self.loader.write_day_ahead_prices(day_ahead_prices_df)
            
            if not br_success or not dap_success:
                self.logger.error("Data loading failed")
                return False
            
            # Log summary
            self._log_processing_summary(date, balancing_reserves_df, day_ahead_prices_df)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing date {date.strftime('%Y-%m-%d')}: {e}")
            return False
    
    def _log_processing_summary(self, date: datetime, br_df, dap_df):
        """
        Log summary of processed data.
        
        Args:
            date: Processed date
            br_df: Balancing reserves DataFrame
            dap_df: Day-ahead prices DataFrame
        """
        self.logger.info(f"Processing summary for {date.strftime('%Y-%m-%d')}:")
        self.logger.info(f"  - Balancing reserves: {len(br_df)} records")
        self.logger.info(f"  - Day-ahead prices: {len(dap_df)} records")
        
        # Log data summaries if available
        if not br_df.empty:
            br_summary = self.transformer.get_data_summary(br_df, 'balancing_reserves')
            self.logger.info(f"  - BR summary: {br_summary}")
        
        if not dap_df.empty:
            dap_summary = self.transformer.get_data_summary(dap_df, 'day_ahead_prices')
            self.logger.info(f"  - DAP summary: {dap_summary}")
    
    def _log_database_stats(self):
        """Log database statistics."""
        try:
            stats = self.loader.get_table_stats()
            self.logger.info("Database statistics:")
            for table, table_stats in stats.items():
                self.logger.info(f"  - {table}: {table_stats}")
        except Exception as e:
            self.logger.warning(f"Failed to get database stats: {e}")
    
    def cleanup(self):
        """Clean up resources."""
        try:
            self.loader.close_connection()
            self.logger.info("Cleanup completed")
        except Exception as e:
            self.logger.error(f"Cleanup failed: {e}")


def main():
    """Main entry point for the ETL pipeline."""
    parser = argparse.ArgumentParser(description="ENTSOE ETL Pipeline")
    parser.add_argument(
        "--mode",
        choices=["daily", "historical"],
        default="daily",
        help="ETL mode: daily (yesterday) or historical (date range)"
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Date in YYYY-MM-DD format or 'daily' for yesterday"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date for historical mode (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date for historical mode (YYYY-MM-DD, defaults to today)"
    )
    parser.add_argument(
        "--country",
        type=str,
        help="Country code (e.g., DE, FR, IT). Defaults to DE"
    )
    
    args = parser.parse_args()
    
    # Create pipeline instance
    pipeline = ENTSOEETLPipeline(country_code=args.country)
    
    try:
        # Run pipeline based on mode
        if args.mode == "daily":
            date_arg = args.date if args.date else "daily"
            success = pipeline.run_daily_etl(date_arg)
        else:  # historical
            success = pipeline.run_historical_etl(args.start_date, args.end_date)
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logging.getLogger("entsoe_etl").info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.getLogger("entsoe_etl").error(f"Pipeline failed: {e}")
        sys.exit(1)
    finally:
        pipeline.cleanup()


if __name__ == "__main__":
    main() 