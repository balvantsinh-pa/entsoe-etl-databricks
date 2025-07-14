"""
Unit tests for ENTSOE API client.
Tests API interactions with mocked responses.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from datetime import datetime, timezone
import xmltodict

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from entsoe_api import ENTSOEAPIClient


class TestENTSOEAPIClient(unittest.TestCase):
    """Test cases for ENTSOEAPIClient."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock settings
        with patch('entsoe_api.settings') as mock_settings:
            mock_settings.entsoe_api_key = 'test_api_key'
            mock_settings.entsoe_base_url = 'https://test.api.com'
            mock_settings.request_timeout = 30
            mock_settings.country_eic = '10Y1001A1001A82H'
            mock_settings.country_code = 'DE'
            
            self.client = ENTSOEAPIClient()
    
    def test_init(self):
        """Test client initialization."""
        self.assertEqual(self.client.api_key, 'test_api_key')
        self.assertEqual(self.client.base_url, 'https://test.api.com')
        self.assertEqual(self.client.timeout, 30)
    
    @patch('entsoe_api.requests.get')
    def test_make_request_success(self, mock_get):
        """Test successful API request."""
        # Mock response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.content = b'<Publication_MarketDocument><TimeSeries></TimeSeries></Publication_MarketDocument>'
        mock_get.return_value = mock_response
        
        # Test request
        params = {'test': 'param'}
        result = self.client._make_request(params)
        
        # Verify
        mock_get.assert_called_once()
        self.assertIn('securityToken', mock_get.call_args[1]['params'])
        self.assertEqual(result['Publication_MarketDocument']['TimeSeries'], [])
    
    @patch('entsoe_api.requests.get')
    def test_make_request_failure(self, mock_get):
        """Test API request failure."""
        # Mock failed response
        mock_get.side_effect = Exception("API Error")
        
        # Test request
        with self.assertRaises(Exception):
            self.client._make_request({'test': 'param'})
    
    @patch.object(ENTSOEAPIClient, '_make_request')
    def test_get_balancing_reserves(self, mock_make_request):
        """Test balancing reserves data extraction."""
        # Mock API response
        mock_response = {
            'Publication_MarketDocument': {
                'TimeSeries': [
                    {
                        'businessType': 'A95',
                        'Period': {
                            'timeInterval': {
                                'start': '2024-01-01T00:00:00Z'
                            },
                            'Point': [
                                {
                                    'position': '1',
                                    'quantity': '100.5'
                                },
                                {
                                    'position': '2',
                                    'quantity': '150.2'
                                }
                            ]
                        }
                    }
                ]
            }
        }
        mock_make_request.return_value = mock_response
        
        # Test extraction
        date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        result = self.client.get_balancing_reserves(date)
        
        # Verify
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]['product'], 'Primary Reserve')
        self.assertEqual(result.iloc[0]['volume_mw'], 100.5)
        self.assertEqual(result.iloc[0]['country_code'], 'DE')
    
    @patch.object(ENTSOEAPIClient, '_make_request')
    def test_get_day_ahead_prices(self, mock_make_request):
        """Test day-ahead prices data extraction."""
        # Mock API response
        mock_response = {
            'Publication_MarketDocument': {
                'TimeSeries': [
                    {
                        'Period': {
                            'timeInterval': {
                                'start': '2024-01-01T00:00:00Z'
                            },
                            'Point': [
                                {
                                    'position': '1',
                                    'price.amount': '50.25'
                                },
                                {
                                    'position': '2',
                                    'price.amount': '75.80'
                                }
                            ]
                        }
                    }
                ]
            }
        }
        mock_make_request.return_value = mock_response
        
        # Test extraction
        date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        result = self.client.get_day_ahead_prices(date)
        
        # Verify
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]['price_eur_per_mwh'], 50.25)
        self.assertEqual(result.iloc[0]['country_code'], 'DE')
    
    def test_extract_product_from_business_type(self):
        """Test business type to product mapping."""
        self.assertEqual(self.client._extract_product_from_business_type('A95'), 'Primary Reserve')
        self.assertEqual(self.client._extract_product_from_business_type('A96'), 'Secondary Reserve')
        self.assertEqual(self.client._extract_product_from_business_type('A97'), 'Tertiary Reserve')
        self.assertEqual(self.client._extract_product_from_business_type('UNKNOWN'), 'UNKNOWN')
    
    def test_extract_time_series_empty(self):
        """Test time series extraction with empty data."""
        empty_data = {}
        result = self.client._extract_time_series(empty_data, 'test')
        self.assertEqual(result, [])
    
    def test_extract_time_series_single(self):
        """Test time series extraction with single series."""
        data = {
            'Publication_MarketDocument': {
                'TimeSeries': {
                    'businessType': 'A95',
                    'Period': {}
                }
            }
        }
        result = self.client._extract_time_series(data, 'test')
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['businessType'], 'A95')
    
    def test_extract_time_series_multiple(self):
        """Test time series extraction with multiple series."""
        data = {
            'Publication_MarketDocument': {
                'TimeSeries': [
                    {'businessType': 'A95'},
                    {'businessType': 'A96'}
                ]
            }
        }
        result = self.client._extract_time_series(data, 'test')
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['businessType'], 'A95')
        self.assertEqual(result[1]['businessType'], 'A96')


class TestENTSOEAPIClientIntegration(unittest.TestCase):
    """Integration tests for ENTSOEAPIClient."""
    
    def setUp(self):
        """Set up test fixtures."""
        with patch('entsoe_api.settings') as mock_settings:
            mock_settings.entsoe_api_key = 'test_api_key'
            mock_settings.entsoe_base_url = 'https://test.api.com'
            mock_settings.request_timeout = 30
            mock_settings.country_eic = '10Y1001A1001A82H'
            mock_settings.country_code = 'DE'
            
            self.client = ENTSOEAPIClient()
    
    @patch.object(ENTSOEAPIClient, '_make_request')
    def test_full_balancing_reserves_workflow(self, mock_make_request):
        """Test complete balancing reserves workflow."""
        # Mock complex API response
        mock_response = {
            'Publication_MarketDocument': {
                'TimeSeries': [
                    {
                        'businessType': 'A95',
                        'Period': {
                            'timeInterval': {
                                'start': '2024-01-01T00:00:00Z'
                            },
                            'Point': [
                                {'position': '1', 'quantity': '100.5'},
                                {'position': '2', 'quantity': '150.2'},
                                {'position': '3', 'quantity': '200.0'}
                            ]
                        }
                    },
                    {
                        'businessType': 'A96',
                        'Period': {
                            'timeInterval': {
                                'start': '2024-01-01T00:00:00Z'
                            },
                            'Point': [
                                {'position': '1', 'quantity': '50.0'},
                                {'position': '2', 'quantity': '75.5'}
                            ]
                        }
                    }
                ]
            }
        }
        mock_make_request.return_value = mock_response
        
        # Test complete workflow
        date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        result = self.client.get_balancing_reserves(date)
        
        # Verify results
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)  # 3 + 2 points
        
        # Check product types
        products = result['product'].unique()
        self.assertIn('Primary Reserve', products)
        self.assertIn('Secondary Reserve', products)
        
        # Check data integrity
        self.assertTrue(all(result['volume_mw'] > 0))
        self.assertTrue(all(result['country_code'] == 'DE'))
    
    @patch.object(ENTSOEAPIClient, '_make_request')
    def test_full_day_ahead_prices_workflow(self, mock_make_request):
        """Test complete day-ahead prices workflow."""
        # Mock complex API response
        mock_response = {
            'Publication_MarketDocument': {
                'TimeSeries': [
                    {
                        'Period': {
                            'timeInterval': {
                                'start': '2024-01-01T00:00:00Z'
                            },
                            'Point': [
                                {'position': '1', 'price.amount': '50.25'},
                                {'position': '2', 'price.amount': '75.80'},
                                {'position': '3', 'price.amount': '45.90'}
                            ]
                        }
                    }
                ]
            }
        }
        mock_make_request.return_value = mock_response
        
        # Test complete workflow
        date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        result = self.client.get_day_ahead_prices(date)
        
        # Verify results
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)
        
        # Check data integrity
        self.assertTrue(all(result['price_eur_per_mwh'] >= 0))
        self.assertTrue(all(result['country_code'] == 'DE'))
        
        # Check datetime progression
        datetimes = result['datetime_utc'].tolist()
        for i in range(1, len(datetimes)):
            self.assertGreater(datetimes[i], datetimes[i-1])


if __name__ == '__main__':
    unittest.main() 