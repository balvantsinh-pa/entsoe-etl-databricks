"""
Configuration module for ENTSOE ETL pipeline.
Loads settings from environment variables with validation.
Databricks-friendly configuration handling.
"""

import os
import json
from typing import Optional, Dict, Any
from dotenv import load_dotenv
from pydantic_settings import BaseSettings
from pydantic import validator

# Load environment variables from .env file (only if it exists)
if os.path.exists('.env'):
    load_dotenv()


class Settings(BaseSettings):
    """Application settings with validation."""
    
    # ENTSOE API Configuration
    entsoe_api_key: str
    entsoe_base_url: str = "https://transparency.entsoe.eu/api"
    entsoe_headers: Dict[str, str] = {"Accept": "application/xml"}
    
    # Database Configuration
    database_url: str
    
    # Country Configuration
    country_eic: str = "10Y1001A1001A82H"  # Germany
    country_code: str = "DE"
    
    # Logging Configuration
    log_level: str = "INFO"
    
    # API Configuration
    max_retries: int = 3
    retry_delay: int = 5
    request_timeout: int = 30
    
    # Databricks-specific configuration
    is_databricks: bool = False
    databricks_workspace_url: Optional[str] = None
    databricks_cluster_id: Optional[str] = None
    
    # ETL Configuration
    batch_size_days: int = 7  # Process data in 1-week chunks
    default_start_date: str = "2024-01-01"
    
    @validator('entsoe_api_key')
    def validate_api_key(cls, v):
        if not v:
            raise ValueError('ENTSOE API key is required')
        return v
    
    @validator('database_url')
    def validate_database_url(cls, v):
        if not v:
            raise ValueError('Database URL is required')
        return v
    
    @validator('log_level')
    def validate_log_level(cls, v):
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f'Log level must be one of: {valid_levels}')
        return v.upper()
    
    def __init__(self, **kwargs):
        # Check if running in Databricks environment
        databricks_env = any([
            'DATABRICKS_RUNTIME_VERSION' in os.environ,
            'SPARK_HOME' in os.environ,
            'DATABRICKS_WORKSPACE_URL' in os.environ
        ])
        
        if databricks_env:
            kwargs['is_databricks'] = True
            kwargs['databricks_workspace_url'] = os.environ.get('DATABRICKS_WORKSPACE_URL')
            kwargs['databricks_cluster_id'] = os.environ.get('DATABRICKS_CLUSTER_ID')
        
        super().__init__(**kwargs)
    
    class Config:
        env_file = ".env"
        case_sensitive = False


def load_country_config() -> Dict[str, Any]:
    """
    Load country configuration from JSON file.
    
    Returns:
        Dictionary with country configurations
    """
    config_path = os.path.join(os.path.dirname(__file__), 'country_config.json')
    
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Country configuration file not found: {config_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in country configuration: {e}")


def get_country_info(country_code: str = None) -> Dict[str, Any]:
    """
    Get information for a specific country.
    
    Args:
        country_code: Country code (e.g., 'DE'). If None, uses default.
    
    Returns:
        Dictionary with country information
    """
    config = load_country_config()
    
    if country_code is None:
        country_code = config.get('default_country', 'DE')
    
    country_info = config['countries'].get(country_code)
    if not country_info:
        raise ValueError(f"Country code '{country_code}' not found in configuration")
    
    return {
        'code': country_code,
        'eic': country_info.get('eic_code'),  # Map eic_code to eic
        **country_info
    }


def get_all_countries() -> Dict[str, Dict[str, Any]]:
    """
    Get all available countries.
    
    Returns:
        Dictionary with all country configurations
    """
    config = load_country_config()
    return config['countries']


def get_api_endpoints() -> Dict[str, str]:
    """
    Get API endpoints from configuration.
    
    Returns:
        Dictionary with API endpoints
    """
    config = load_country_config()
    return config.get('api_endpoints', {})


def get_databricks_info() -> Dict[str, Any]:
    """
    Get Databricks environment information.
    
    Returns:
        Dictionary with Databricks info
    """
    return {
        'workspace_url': os.environ.get('DATABRICKS_WORKSPACE_URL'),
        'cluster_id': os.environ.get('DATABRICKS_CLUSTER_ID'),
        'runtime_version': os.environ.get('DATABRICKS_RUNTIME_VERSION'),
        'is_databricks': settings.is_databricks
    }


# Global settings instance
settings = Settings() 