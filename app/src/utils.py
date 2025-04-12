import os # operating system operations like check files existance
from pathlib import Path
import gc # garbage collector
import time # time related utility functions
import getpass
import traceback
import requests

import pandas as pd # data frames wrangling
import numpy as np # fast and vectorized math functions
import scipy as sp # scientific calculation toolkit

#import matplotlib.pyplot as plt # MATLAB-like plotting library
#import matplotlib.dates as mdates
#import matplotlib.font_manager as fm
#import plotly.graph_objects as go
#from plotly.subplots import make_subplots
#plt.rcParams["figure.figsize"] = (14, 12)
#plt.rcParams['axes.unicode_minus'] = False  # This line is to ensure the minus sign displays correctly

from dateutil.relativedelta import relativedelta
#import datetime
from datetime import datetime,timedelta,timezone
import math
import re


### Binance Options API
import hmac
import hashlib
import urllib.parse

from typing import Optional, Dict, List, Union, Any

class BinanceOptionsClient:
    """
    Client for interacting with Binance Options API
    """
    
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        """
        Initialize the Binance Options client
        
        Parameters:
        - api_key: Your Binance API key
        - api_secret: Your Binance API secret
        """
        self.base_url = 'https://eapi.binance.com'
        self.api_key = api_key
        self.api_secret = api_secret
        self.headers = {
            'X-MBX-APIKEY': self.api_key
        } if self.api_key else {}
        
    def _generate_signature(self, params: Dict[str, Any]) -> str:
        """
        Generate signature for authenticated requests
        
        Parameters:
        - params: Parameters to sign
        
        Returns:
        - Signature string
        """
        if not self.api_secret:
            raise ValueError("API secret is required for authenticated requests")
            
        # Sort parameters alphabetically
        sorted_params = dict(sorted(params.items()))
        
        # Create query string
        query_string = urllib.parse.urlencode(sorted_params)
        
        # Generate signature
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        return signature
    
    def _make_request(self, method: str, endpoint: str, params: Optional[Dict[str, Any]] = None, 
                     signed: bool = False) -> Dict[str, Any]:
        """
        Make HTTP request to Binance API
        
        Parameters:
        - method: HTTP method (GET, POST, etc.)
        - endpoint: API endpoint
        - params: Request parameters
        - signed: Whether the request needs to be signed
        
        Returns:
        - API response as dictionary
        """
        url = self.base_url + endpoint
        
        if params is None:
            params = {}
            
        # Add timestamp for signed requests
        if signed:
            params['timestamp'] = int(time.time() * 1000)
            params['signature'] = self._generate_signature(params)
            
        try:
            if method == 'GET':
                response = requests.get(url, params=params, headers=self.headers)
            elif method == 'POST':
                response = requests.post(url, data=params, headers=self.headers)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
                
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error making request: {e}")
            if hasattr(e.response, 'text'):
                print(f"Response: {e.response.text}")
            raise
    
    def get_exchange_info(self) -> Dict[str, Any]:
        """
        Get exchange information
        
        Returns:
        - Exchange information including option contracts, assets, and symbols
        - unit: // Contract unit, the quantity of the underlying asset represented by a single contract.
        - priceScale: // price precision
        """
        return self._make_request('GET', '/eapi/v1/exchangeInfo')
    
    
    def get_exercise_history(self, underlying: Optional[str] = None, 
                           startTime: Optional[Union[int, str, datetime]] = None,
                           endTime: Optional[Union[int, str, datetime]] = None,
                           limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get historical exercise records
        
        Parameters:
        - underlying: Underlying asset (e.g., 'BTCUSDT')
        - startTime: Start time for historical data
        - endTime: End time for historical data
        - limit: Number of records to return (default: 100, max: 100)
        
        Returns:
        - List of exercise history records
        """
        params = {'limit': min(limit, 100)}  # Ensure limit doesn't exceed 100
        
        if underlying:
            params['underlying'] = underlying
            
        if startTime:
            if isinstance(startTime, (str, datetime)):
                params['startTime'] = int(pd.Timestamp(startTime).timestamp() * 1000)
            else:
                params['startTime'] = startTime
                
        if endTime:
            if isinstance(endTime, (str, datetime)):
                params['endTime'] = int(pd.Timestamp(endTime).timestamp() * 1000)
            else:
                params['endTime'] = endTime
                
        return self._make_request('GET', '/eapi/v1/exerciseHistory', params)
    
    def get_open_interest(self, underlyingAsset: str, expiration: str) -> List[Dict[str, Any]]:
        """
        Get open interest for specific underlying asset on specific expiration date
        
        Parameters:
        - underlyingAsset: Underlying asset, e.g. ETH/BTC
        - expiration: Expiration date, e.g. 221225
        
        Returns:
        - List of open interest records
        """
        params = {
            'underlyingAsset': underlyingAsset,
            'expiration': expiration
        }
        
        return self._make_request('GET', '/eapi/v1/openInterest', params)
        
    def get_depth(self, symbol: str, limit: int = 100) -> Dict[str, Any]:
        """
        Get order book depth
        
        Parameters:
        - symbol: Option symbol (e.g., 'BTC-250627-55000-C')
        - limit: Number of records to return (max 100)
        
        Returns:
        - Order book depth data
        """
        params = {
            'symbol': symbol,
            'limit': limit
        }
        
        return self._make_request('GET', '/eapi/v1/depth', params)
    
    def get_klines(self, symbol: str, 
                  interval: str = '1m',
                  startTime: Optional[Union[int, str, datetime]] = None,
                  endTime: Optional[Union[int, str, datetime]] = None,
                  limit: int = 500) -> List[List[Any]]:
        """
        Get klines/candlestick data
        
        Parameters:
        - symbol: Option symbol (e.g., 'BTC-250627-55000-C')
        - interval: Kline interval (15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M)
        - startTime: Start time for historical data
        - endTime: End time for historical data
        - limit: Number of records to return (default 500 max 1500)
        
        Returns:
        - List of kline data
        """
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': min(limit, 1500)
        }
        
        if startTime:
            if isinstance(startTime, (str, datetime)):
                params['startTime'] = int(pd.Timestamp(startTime).timestamp() * 1000)
            else:
                params['startTime'] = startTime
                
        if endTime:
            if isinstance(endTime, (str, datetime)):
                params['endTime'] = int(pd.Timestamp(endTime).timestamp() * 1000)
            else:
                params['endTime'] = endTime
                
        return self._make_request('GET', '/eapi/v1/klines', params)
    
    def get_mark_price(self, symbol: str) -> Dict[str, Any]:
        """
        Get mark price for an option
        
        Parameters:
        - symbol: Option symbol (e.g., 'BTC-250627-55000-C')
        
        Returns:
        - Mark price data
        """
        params = {'symbol': symbol}
        return self._make_request('GET', '/eapi/v1/mark', params)
    

    def get_historical_trades(self, symbol: str, limit: int = 100, fromId: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get historical trades
        
        Parameters:
        - symbol: Option symbol (e.g., 'BTC-250627-55000-C')
        - limit: Number of records to return (default: 100, max: 500)
        - from_id: The UniqueId ID from which to return. The latest deal record is returned by default
        
        Returns:
        - List of historical trades
        """
        params = {
            'symbol': symbol,
            'limit': min(limit, 500)  # Ensure limit doesn't exceed 500
        }
        
        if fromId is not None:
            params['fromId'] = fromId
        
        return self._make_request('GET', '/eapi/v1/historicalTrades', params)
    

'''
from dotenv import load_dotenv
load_dotenv()
client = BinanceOptionsClient(api_key=os.environ['BINANCE_API_KEY'], api_secret=os.environ['BINANCE_API_SECRET'])
exercise_history = client.get_exercise_history(
    underlying='BTCUSDT',
    startTime=int(pd.Timestamp('2025-04-03 08:00:00').timestamp() * 1000),
    endTime=int(pd.Timestamp('2025-04-05 08:00:00').timestamp() * 1000)
)
df = pd.DataFrame(exercise_history)
df.expiryDate = pd.to_datetime(df.expiryDate, unit='ms')
print(df)


klines = client.get_klines(
    symbol='BTC-250404-80000-C', 
    interval='1h', 
    startTime=int(pd.Timestamp('2025-03-28 08:00:00').timestamp() * 1000), 
    endTime=int(pd.Timestamp('2025-04-05 08:00:00').timestamp() * 1000), 
    limit=100
)
df = pd.DataFrame(klines)

print(df.dtypes)
df.openTime = pd.to_datetime(df.openTime, unit='ms')
df.closeTime = pd.to_datetime(df.closeTime, unit='ms')
print(df)





'''