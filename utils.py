"""
Utility functions for ENTSOE ETL pipeline.
Includes date handling, logging, and retry mechanisms.
Databricks-friendly utilities.
"""

import logging
import sys
import os
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional
from tenacity import retry, stop_after_attempt, wait_exponential
import pytz

from config import settings


def setup_logging(level: str = "INFO") -> logging.Logger:
    """
    Set up logging configuration.
    Databricks-friendly logging setup.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger("entsoe_etl")
    logger.setLevel(getattr(logging, level.upper()))
    
    # Remove existing handlers to avoid duplicates
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, level.upper()))
    
    # Create formatter - Databricks-friendly format
    if settings.is_databricks:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
    
    console_handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(console_handler)
    
    return logger


def get_databricks_file_path(relative_path: str) -> str:
    """
    Get absolute file path for Databricks environment.
    
    Args:
        relative_path: Relative path from repository root
    
    Returns:
        Absolute path suitable for Databricks
    """
    if settings.is_databricks:
        # In Databricks, files are typically in /Workspace/Repos/...
        workspace_path = os.environ.get('DATABRICKS_WORKSPACE_PATH', '/Workspace')
        repo_name = os.environ.get('DATABRICKS_REPO_NAME', 'entsoe-etl-databricks')
        return os.path.join(workspace_path, 'Repos', repo_name, relative_path)
    else:
        # Local development
        return os.path.abspath(relative_path)


def get_date_range(start_date: str, end_date: str) -> List[datetime]:
    """
    Generate a list of dates between start_date and end_date (inclusive).
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
    
    Returns:
        List of datetime objects
    """
    start = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    
    dates = []
    current = start
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    
    return dates


def get_yesterday_utc() -> datetime:
    """
    Get yesterday's date in UTC.
    
    Returns:
        Yesterday's datetime in UTC
    """
    return datetime.now(timezone.utc) - timedelta(days=1)


def normalize_utc_time(dt: datetime, timezone_str: str = "Europe/Berlin") -> datetime:
    """
    Normalize datetime to UTC, handling DST conversion.
    
    Args:
        dt: Datetime object (assumed to be in local timezone)
        timezone_str: Timezone string (default: Europe/Berlin for Germany)
    
    Returns:
        Datetime object in UTC
    """
    if dt.tzinfo is None:
        # Assume local timezone if no timezone info
        local_tz = pytz.timezone(timezone_str)
        dt = local_tz.localize(dt)
    
    return dt.astimezone(timezone.utc)


def parse_date_argument(date_arg: str) -> Tuple[datetime, datetime]:
    """
    Parse date argument and return start/end dates.
    
    Args:
        date_arg: Date string or "daily" for yesterday
    
    Returns:
        Tuple of (start_date, end_date) as datetime objects
    """
    if date_arg.lower() == "daily":
        yesterday = get_yesterday_utc()
        return yesterday, yesterday
    
    try:
        # Single date
        date_obj = datetime.strptime(date_arg, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return date_obj, date_obj
    except ValueError:
        raise ValueError(f"Invalid date format: {date_arg}. Use YYYY-MM-DD or 'daily'")


def safe_float(value: str) -> Optional[float]:
    """
    Safely convert string to float, returning None if conversion fails.
    
    Args:
        value: String value to convert
    
    Returns:
        Float value or None if conversion fails
    """
    try:
        return float(value) if value else None
    except (ValueError, TypeError):
        return None


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
def retry_function(func, *args, **kwargs):
    """
    Retry wrapper for functions that may fail due to network issues.
    
    Args:
        func: Function to retry
        *args: Function arguments
        **kwargs: Function keyword arguments
    
    Returns:
        Function result
    
    Raises:
        Exception: If all retries fail
    """
    return func(*args, **kwargs)


def validate_dataframe(df, required_columns: List[str]) -> bool:
    """
    Validate DataFrame has required columns and is not empty.
    
    Args:
        df: Pandas DataFrame to validate
        required_columns: List of required column names
    
    Returns:
        True if valid, False otherwise
    """
    if df is None or df.empty:
        return False
    
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        return False
    
    return True


def get_databricks_info() -> dict:
    """
    Get Databricks environment information.
    
    Returns:
        Dictionary with Databricks environment details
    """
    info = {
        'is_databricks': settings.is_databricks,
        'runtime_version': os.environ.get('DATABRICKS_RUNTIME_VERSION'),
        'workspace_url': os.environ.get('DATABRICKS_WORKSPACE_URL'),
        'cluster_id': os.environ.get('DATABRICKS_CLUSTER_ID'),
        'spark_home': os.environ.get('SPARK_HOME'),
        'python_version': sys.version,
        'working_directory': os.getcwd()
    }
    return info 