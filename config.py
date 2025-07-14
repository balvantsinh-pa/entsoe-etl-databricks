"""
Configuration module for ENTSOE ETL pipeline.
Loads settings from environment variables with validation.
"""

import os
from typing import Optional
from dotenv import load_dotenv
from pydantic import BaseSettings, validator

# Load environment variables from .env file
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
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Global settings instance
settings = Settings() 