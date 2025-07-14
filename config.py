"""
Configuration module for ENTSOE ETL pipeline.
Loads settings from environment variables with validation.
Databricks-friendly configuration handling.
"""

import os
from typing import Optional
from dotenv import load_dotenv
from pydantic import BaseSettings, validator

# Load environment variables from .env file (only if it exists)
if os.path.exists('.env'):
    load_dotenv()


class Settings(BaseSettings):
    """Application settings with validation."""
    
    # ENTSOE API Configuration
    entsoe_api_key: str
    entsoe_base_url: str = "https://transparency.entsoe.eu/api"
    
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


# Global settings instance
settings = Settings() 