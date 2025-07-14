"""
ENTSOE API client for fetching energy market data.
Handles balancing reserves and day-ahead prices for Germany.
"""

import logging
import requests
import xmltodict
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from tenacity import retry, stop_after_attempt, wait_exponential

from config import settings
from utils import retry_function, safe_float


class ENTSOEAPIClient:
    """Client for interacting with ENTSOE Transparency Platform API."""
    
    def __init__(self):
        self.api_key = settings.entsoe_api_key
        self.base_url = settings.entsoe_base_url
        self.timeout = settings.request_timeout
        self.logger = logging.getLogger("entsoe_etl.entsoe_api")
    
    def _make_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Make HTTP request to ENTSOE API with retry logic.
        
        Args:
            params: Query parameters for the API request
        
        Returns:
            API response as dictionary
        
        Raises:
            requests.RequestException: If API request fails
        """
        try:
            # Add API key to parameters
            params['securityToken'] = self.api_key
            
            self.logger.debug(f"Making API request with params: {params}")
            
            response = retry_function(
                requests.get,
                self.base_url,
                params=params,
                timeout=self.timeout
            )
            
            response.raise_for_status()
            
            # Parse XML response
            data = xmltodict.parse(response.content)
            return data
            
        except requests.RequestException as e:
            self.logger.error(f"API request failed: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error during API request: {e}")
            raise
    
    def get_balancing_reserves(self, date: datetime) -> pd.DataFrame:
        """
        Fetch balancing reserves data for a specific date.
        
        Args:
            date: Date to fetch data for (datetime object)
        
        Returns:
            DataFrame with balancing reserves data
        """
        self.logger.info(f"Fetching balancing reserves for {date.strftime('%Y-%m-%d')}")
        
        # Format date for API
        start_date = date.strftime('%Y%m%d0000')
        end_date = (date + pd.Timedelta(days=1)).strftime('%Y%m%d0000')
        
        params = {
            'documentType': 'A73',  # Balancing reserves
            'in_Domain': settings.country_eic,
            'out_Domain': settings.country_eic,
            'periodStart': start_date,
            'periodEnd': end_date
        }
        
        try:
            data = self._make_request(params)
            
            # Extract time series data
            time_series = self._extract_time_series(data, 'balancing_reserves')
            
            if not time_series:
                self.logger.warning(f"No balancing reserves data found for {date.strftime('%Y-%m-%d')}")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = self._parse_balancing_reserves(time_series, date)
            
            self.logger.info(f"Retrieved {len(df)} balancing reserves records")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to fetch balancing reserves: {e}")
            raise
    
    def get_day_ahead_prices(self, date: datetime) -> pd.DataFrame:
        """
        Fetch day-ahead prices data for a specific date.
        
        Args:
            date: Date to fetch data for (datetime object)
        
        Returns:
            DataFrame with day-ahead prices data
        """
        self.logger.info(f"Fetching day-ahead prices for {date.strftime('%Y-%m-%d')}")
        
        # Format date for API
        start_date = date.strftime('%Y%m%d0000')
        end_date = (date + pd.Timedelta(days=1)).strftime('%Y%m%d0000')
        
        params = {
            'documentType': 'A44',  # Day-ahead prices
            'in_Domain': settings.country_eic,
            'out_Domain': settings.country_eic,
            'periodStart': start_date,
            'periodEnd': end_date
        }
        
        try:
            data = self._make_request(params)
            
            # Extract time series data
            time_series = self._extract_time_series(data, 'day_ahead_prices')
            
            if not time_series:
                self.logger.warning(f"No day-ahead prices data found for {date.strftime('%Y-%m-%d')}")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = self._parse_day_ahead_prices(time_series, date)
            
            self.logger.info(f"Retrieved {len(df)} day-ahead prices records")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to fetch day-ahead prices: {e}")
            raise
    
    def _extract_time_series(self, data: Dict[str, Any], data_type: str) -> List[Dict[str, Any]]:
        """
        Extract time series data from API response.
        
        Args:
            data: API response data
            data_type: Type of data being extracted
        
        Returns:
            List of time series data
        """
        try:
            # Navigate through the XML structure
            publication_market_document = data.get('Publication_MarketDocument', {})
            time_series = publication_market_document.get('TimeSeries', [])
            
            # Ensure time_series is always a list
            if not isinstance(time_series, list):
                time_series = [time_series]
            
            self.logger.debug(f"Extracted {len(time_series)} time series for {data_type}")
            return time_series
            
        except Exception as e:
            self.logger.error(f"Failed to extract time series for {data_type}: {e}")
            return []
    
    def _parse_balancing_reserves(self, time_series: List[Dict[str, Any]], date: datetime) -> pd.DataFrame:
        """
        Parse balancing reserves time series data into DataFrame.
        
        Args:
            time_series: List of time series data
            date: Date for the data
        
        Returns:
            DataFrame with balancing reserves data
        """
        records = []
        
        for series in time_series:
            try:
                # Extract product type
                business_type = series.get('businessType', '')
                product = self._extract_product_from_business_type(business_type)
                
                # Extract period data
                period = series.get('Period', {})
                points = period.get('Point', [])
                
                if not isinstance(points, list):
                    points = [points]
                
                for point in points:
                    try:
                        position = int(point.get('position', 0))
                        volume = safe_float(point.get('quantity'))
                        
                        # Calculate datetime from position
                        period_start = period.get('timeInterval', {}).get('start', '')
                        if period_start:
                            start_dt = datetime.fromisoformat(period_start.replace('Z', '+00:00'))
                            dt = start_dt + pd.Timedelta(hours=position-1)
                            
                            records.append({
                                'country_code': settings.country_code,
                                'datetime_utc': dt,
                                'product': product,
                                'volume_mw': volume,
                                'price_eur_per_mw': None  # Not available in balancing reserves
                            })
                    
                    except (ValueError, KeyError) as e:
                        self.logger.warning(f"Failed to parse balancing reserves point: {e}")
                        continue
            
            except Exception as e:
                self.logger.warning(f"Failed to parse balancing reserves series: {e}")
                continue
        
        return pd.DataFrame(records)
    
    def _parse_day_ahead_prices(self, time_series: List[Dict[str, Any]], date: datetime) -> pd.DataFrame:
        """
        Parse day-ahead prices time series data into DataFrame.
        
        Args:
            time_series: List of time series data
            date: Date for the data
        
        Returns:
            DataFrame with day-ahead prices data
        """
        records = []
        
        for series in time_series:
            try:
                # Extract period data
                period = series.get('Period', {})
                points = period.get('Point', [])
                
                if not isinstance(points, list):
                    points = [points]
                
                for point in points:
                    try:
                        position = int(point.get('position', 0))
                        price = safe_float(point.get('price.amount'))
                        
                        # Calculate datetime from position
                        period_start = period.get('timeInterval', {}).get('start', '')
                        if period_start:
                            start_dt = datetime.fromisoformat(period_start.replace('Z', '+00:00'))
                            dt = start_dt + pd.Timedelta(hours=position-1)
                            
                            records.append({
                                'country_code': settings.country_code,
                                'datetime_utc': dt,
                                'price_eur_per_mwh': price
                            })
                    
                    except (ValueError, KeyError) as e:
                        self.logger.warning(f"Failed to parse day-ahead price point: {e}")
                        continue
            
            except Exception as e:
                self.logger.warning(f"Failed to parse day-ahead prices series: {e}")
                continue
        
        return pd.DataFrame(records)
    
    def _extract_product_from_business_type(self, business_type: str) -> str:
        """
        Extract product type from business type.
        
        Args:
            business_type: Business type string from API
        
        Returns:
            Product type string
        """
        # Map business types to product names
        product_mapping = {
            'A95': 'Primary Reserve',
            'A96': 'Secondary Reserve',
            'A97': 'Tertiary Reserve',
            'A98': 'Manual Frequency Restoration Reserve',
            'A99': 'Automatic Frequency Restoration Reserve'
        }
        
        return product_mapping.get(business_type, business_type) 