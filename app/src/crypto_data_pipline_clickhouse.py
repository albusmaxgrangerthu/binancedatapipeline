from dataclasses import dataclass
from typing import Dict, List, Any, Callable, Optional, Union

from utils_clickhouse import *

from binance.spot import Spot
from binance.um_futures import UMFutures
import logging

import pandas_ta as ta  # Much simpler with pandas_ta

from ratelimit import limits, sleep_and_retry
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

from concurrent.futures import ThreadPoolExecutor
import threading

# REST API for Historical Data
class BinanceDataFetcher:
    # Add class constants for rate limits
    SPOT_WEIGHT_LIMIT = 5500    # requests per minute for spot
    SPOT_KLINE_WEIGHT = 2
    FUTURES_WEIGHT_LIMIT = 2300 # requests per minute for futures
    FUTURES_KLINE_WEIGHT = 2
    RATE_LIMIT_PERIOD = 60    # seconds

    FR_LIMIT = 1000
    FR_PERIOD = 300

    MR_LIMIT = 1000
    MR_PERIOD = 60
    
    BATCH_SIZE = 20          # symbols per batch
    FUTURES_MAX_WORKERS = 8
    SPOT_MAX_WORKERS = 10

    SYMBOL_DELAY = 0.05       # 150ms between symbols
    BATCH_DELAY = 0.5         # 500ms between batches
    
    
    def __init__(self, con: Client, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        """
        Initialize Binance data fetcher with optional API credentials
        """
        self.spot_client = Spot(api_key=api_key, api_secret=api_secret)
        self.um_futures_client = UMFutures(key=api_key, secret=api_secret)
        self.con = con

        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True
    )
    def get_spot_symbols(self) -> pd.DataFrame:
        """
        Fetch all available spot trading pairs
        Returns DataFrame with symbol details
        """
        try:
            # Request SPOT exchange information
            exchange_info = self.spot_client.exchange_info(permissions=['SPOT'])
            symbols_data = []
            
            for symbol in exchange_info['symbols']:
                # Get filter information
                price_filter = next((f for f in symbol['filters'] if f['filterType'] == 'PRICE_FILTER'), None)
                lot_size = next((f for f in symbol['filters'] if f['filterType'] == 'LOT_SIZE'), None)
                
                symbol_info = {
                    'symbol': symbol['symbol'],
                    'base_asset': symbol['baseAsset'],
                    'quote_asset': symbol['quoteAsset'],
                    'exchange': 'binance',
                    'type': 'SPOT',
                    'status': symbol['status'],
                    'is_spot_trading_allowed': symbol['isSpotTradingAllowed'],
                    'is_margin_trading_allowed': symbol['isMarginTradingAllowed'],
                    
                    # precision
                    'base_precision': symbol['baseAssetPrecision'],
                    'quote_precision': symbol['quoteAssetPrecision'],
                    'min_price': float(price_filter['minPrice']) if price_filter else None,
                    'max_price': float(price_filter['maxPrice']) if price_filter else None,
                    'tick_size': float(price_filter['tickSize']) if price_filter else None,
                    'min_qty': float(lot_size['minQty']) if lot_size else None,
                    'max_qty': float(lot_size['maxQty']) if lot_size else None,
                    'step_size': float(lot_size['stepSize']) if lot_size else None
                }
                symbols_data.append(symbol_info)
                
            df = pd.DataFrame(symbols_data)
            self.logger.info(f"Fetched {len(df)} spot symbols")
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching spot symbols: {e}")
            raise
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True
    )
    def get_um_perpetual_symbols(self) -> pd.DataFrame:
        """
        Fetch all USD-M perpetual futures trading pairs
        Returns DataFrame with symbol details
        """
        try:
            exchange_info = self.um_futures_client.exchange_info()
            perp_data = []
            
            for symbol in exchange_info['symbols']:
                if symbol['contractType'] == 'PERPETUAL':
                    symbol_info = {
                        'symbol': symbol['symbol'],
                        'base_asset': symbol['baseAsset'],
                        'quote_asset': symbol['quoteAsset'],
                        'margin_asset': symbol['marginAsset'], 
                        'exchange': 'binance',
                        'type': symbol['contractType'],
                        'underlyingSubType': ','.join(symbol['underlyingSubType']),
                        'status': symbol['status'],                       
                        'onboard_date': symbol['onboardDate'],
                        'delivery_date': symbol['deliveryDate'],
                        
                        # precision
                        'price_precision': symbol['pricePrecision'],
                        'quantity_precision': symbol['quantityPrecision'],  
                        # Market Order Protection
                        # trigger protect; place order vs execute order 条件触发止盈止损单保护; STOP_MARKET, TAKE_PROFIT_MARKET 
                        # marketTakeBound; 市价单保护; direct take MARKET                     
                        # Limit Order Filter
                        'min_price': float(symbol['filters'][0]['minPrice']),
                        'max_price': float(symbol['filters'][0]['maxPrice']),
                        'tick_size': float(symbol['filters'][0]['tickSize']),
                        'min_qty': float(symbol['filters'][1]['minQty']),
                        'max_qty': float(symbol['filters'][1]['maxQty']),
                        'step_size': float(symbol['filters'][1]['stepSize'])
                        
                    }
                    perp_data.append(symbol_info)
            
            df = pd.DataFrame(perp_data)
            # the dates are both in UTC tz
            df['delivery_date'] = pd.to_datetime(df['delivery_date'], unit='ms')
            df['onboard_date'] = pd.to_datetime(df['onboard_date'], unit='ms')
            self.logger.info(f"Fetched {len(df)} perpetual futures symbols")
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching perpetual futures symbols: {e}")
            raise
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True
    )
    @sleep_and_retry
    @limits(calls=MR_LIMIT, period=MR_PERIOD)
    def _rate_limited_margin_interest_rates(self, asset: str, **kwargs) -> List:
        """Base rate-limited margin interest rate API call"""
        try:
            return self.spot_client.margin_interest_rate_history(asset=asset, **kwargs)
        except Exception as e:
            # Handle specific error codes first
            if '-11027' in str(e):  # Asset not supported
                self.logger.info(f"Asset {asset} not supported for margin interest rate")
                return []
            
            self._handle_rate_limit_error(e)
            # Handle rate limits
            if '418' in str(e):
                return self._rate_limited_margin_interest_rates(asset, **kwargs)
                
            # Other errors
            raise
    
    def get_margin_interest_rate(self, 
                        asset: Optional[str] = None,
                        start_time: int = None,
                        end_time: int = None,
                        vip_level: int = 0) -> pd.DataFrame:
        """
        Fetch margin interest rate history for spot margin
        """
        try:           
            margin_interest_rates = self._rate_limited_margin_interest_rates(
                asset=asset,
                startTime=start_time,
                endTime=end_time,
                vipLevel=vip_level
            )
            
            if not margin_interest_rates:
                #self.logger.info(f"Asset {asset} not supported for margin interest rate from {pd.to_datetime(start_time, unit='ms')} to {pd.to_datetime(end_time, unit='ms')}")
                return pd.DataFrame()
            
            df = pd.DataFrame(margin_interest_rates)           
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching margin interest rate data for {asset}: {e}")
            raise

    def get_historical_margin_interest_rate(self,
                              asset: str,
                              start_time: Union[str, datetime, int],
                              end_time: Union[str, datetime, int],
                              list_date: Union[str, datetime, int] = None,
                              delist_date: Union[str, datetime, int] = None,
                              vip_level: int = 0) -> pd.DataFrame:
        """
        Fetch historical margin interest rate data by making multiple requests if needed
        Handles rate limiting and pagination
        
        Parameters:
        - asset: Trading asset
        - start_time: Start time for historical data
        - end_time: End time for historical data
        - delivery_date: Delivery date for spot
        - vip_level: VIP level for margin interest rate
        
        Returns:
        - DataFrame with margin interest rate history
        """
        try:
            all_margin_interest_rates = []
            
            # Convert times to milliseconds timestamp
            if isinstance(start_time, (str, datetime)):
                start_time_ms = int(pd.Timestamp(start_time).timestamp() * 1000)
            if isinstance(end_time, (str, datetime)):
                end_time_ms = int(pd.Timestamp(end_time).timestamp() * 1000)

            if list_date and isinstance(list_date, (str, datetime)):
                list_date_ms = int(pd.Timestamp(list_date).timestamp() * 1000)
                start_time_ms = max(list_date_ms, start_time_ms)

            if delist_date and isinstance(delist_date, (str, datetime)):
                # Convert delivery_date to UTC timestamp in milliseconds
                delist_date_ms = int(pd.Timestamp(delist_date).timestamp() * 1000)
                
                # Compare millisecond timestamps directly
                end_time_ms = min(delist_date_ms, end_time_ms)
            
            current_start_ms = start_time_ms
            while current_start_ms <= end_time_ms:                
                # time.sleep(self.PAGINATION_DELAY)
                #print(f'''{asset} from {pd.to_datetime(current_start_ms, unit='ms')} to {pd.to_datetime(min(current_start_ms + 30 * 24 * 60 * 60 * 1000, end_time_ms), unit='ms')}''')
                df = self.get_margin_interest_rate(
                    asset=asset,
                    start_time=current_start_ms,
                    end_time=min(current_start_ms + 30 * 24 * 60 * 60 * 1000, end_time_ms),
                    vip_level=vip_level
                )
                
                if not df.empty:
                    all_margin_interest_rates.append(df)
                
                # Update start_time for next request; descending order 取第一行
                current_start_ms = min(current_start_ms + 30 * 24 * 60 * 60 * 1000, end_time_ms) + 1
            
            if all_margin_interest_rates:
                result = pd.concat(all_margin_interest_rates, axis=0)
                result = (result                    
                        .drop_duplicates(subset=['asset', 'timestamp'], keep='last')
                        .sort_values(['asset', 'timestamp'])
                        .reset_index(drop=True)
                        )
                
                return result
                
            return pd.DataFrame()
            
        except Exception as e:
            self.logger.error(f"Error fetching historical margin interest rate data for {asset}: {e}")
            raise

    def fetch_market_margin_interest_rates_threadpool(self,
                                start_time: Union[str, datetime, int],
                                end_time: Union[str, datetime, int],
                                vip_level: int = 0) -> pd.DataFrame:
        """Fetch margin interest rates using thread pool"""
        try:
            # Get current perpetual symbols
        
            symbols_df = clickhouse_query(self.con, 
                '''
                    with delivery_date as ( 
                        select 
                            symbol, 
                            min(timestamp) as list_date,
                            max(timestamp) as delist_date
                        from bn_spot_klines
                        group by symbol
                    )
                    select 
                        distinct s.base_asset as asset, 
                        d.list_date,
                        d.delist_date
                    from delivery_date d
                    inner join bn_spot_symbols s
                        on d.symbol = s.symbol
                    where s.base_asset not in ('TUSD', 'XUSD', 'WBTC', 'WBETH', 'BNSOL', 'USDP')
                ''')
            udst_df = pd.DataFrame({
                'asset': 'USDT',
                'list_date': symbols_df['list_date'].min(),
                'delist_date': symbols_df['delist_date'].max()
            }, index=[0])
            symbols_df = pd.concat([symbols_df, udst_df], ignore_index=True)

            # Calculate optimal thread count based on rate limit
            # 500 requests per 5 minutes = 100 requests per minute
            # Use conservative number of threads
            max_workers = self.SPOT_MAX_WORKERS

            self.logger.info(
                f"Fetching margin interest rates for {len(symbols_df)} symbols "
                f"using {max_workers} threads"
            )

            # Set default time range if not provided
            if end_time is None:
                end_time = datetime.now(timezone.utc)
            if start_time is None:
                start_time = datetime(2025, 1, 20)

            # Shared result containers with thread safety
            all_results = []
            failed_symbols = []
            result_lock = threading.Lock()

            def process_symbol(asset: str, list_date: str, delist_date: str) -> None:
                """Thread worker function"""
                try:
                    rates = self.get_historical_margin_interest_rate(
                        asset=asset,
                        start_time=start_time,
                        end_time=end_time,
                        list_date=list_date,
                        delist_date=delist_date,
                        vip_level=vip_level
                    )

                    if rates is not None and not rates.empty:
                        with result_lock:
                            all_results.append(rates)
                            self.logger.info(
                                f"Fetched {len(rates)} margin interest rates for {asset} "
                                f"from {pd.to_datetime(rates['timestamp'].min(), unit='ms')} to {pd.to_datetime(rates['timestamp'].max(), unit='ms')}"
                            )

                except Exception as e:
                    if '418' in str(e):
                        self._handle_rate_limit_error(e)
                        # Retry after IP ban
                        return process_symbol(asset, list_date, delist_date)
                    with result_lock:
                        failed_symbols.append(asset)
                        self.logger.error(f"Error fetching margin interest rates for {asset}: {e}")

            # Process symbols using thread pool
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []

                # Submit all symbols to thread pool
                for _, row in symbols_df.iterrows():
                    asset = row['asset']
                    list_date = row['list_date']
                    delist_date = row['delist_date']
                    futures.append(
                        executor.submit(process_symbol, asset, list_date, delist_date)
                    )

                # Show progress bar while waiting for completion
                with tqdm(total=len(futures), desc="Fetching margin interest rates") as pbar:
                    for future in futures:
                        future.result()  # Wait for completion
                        pbar.update(1)

            # Report failed symbols
            if failed_symbols:
                self.logger.warning(
                    f"Failed to fetch margin interest rates for {len(failed_symbols)} symbols: {failed_symbols}"
                )

            # Process final results
            if all_results:
                result = pd.concat(all_results, axis=0)
                result = (result
                        .drop_duplicates(subset=['asset', 'timestamp'], keep='last')
                        .sort_values(['asset', 'timestamp'])
                        .reset_index(drop=True))

                if not result.empty:
                    result['timestamp'] = pd.to_datetime(result['timestamp'], unit='ms')
                    result['dailyInterestRate'] = result['dailyInterestRate'].astype(float)
                    result['exchange'] = 'binance'
                    result['type'] = 'Margin'

                    # Reorder columns
                    columns = ['asset', 'exchange', 'type', 'timestamp', 'dailyInterestRate','vipLevel']
                    return result[columns]

            return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"Error in fetch_market_margin_interest_rates: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True
    )
    @sleep_and_retry
    @limits(calls=FR_LIMIT, period=FR_PERIOD)  # Conservative API-level limit for funding rate endpoint
    def _rate_limited_funding_rates(self, symbol: str, **kwargs) -> List:
        """Base rate-limited funding API call"""
        try:
            return self.um_futures_client.funding_rate(symbol=symbol, **kwargs)
        except Exception as e:
            self._handle_rate_limit_error(e)
            # After waiting for IP ban, retry the request
            if '418' in str(e):
                return self._rate_limited_funding_rates(symbol, **kwargs)
            raise
    
    def get_funding_rate(self, 
                        symbol: Optional[str] = None,
                        start_time: int = None,
                        end_time: int = None,
                        limit: int = 1000) -> pd.DataFrame:
        """
        Fetch funding rate history for USD-M perpetual futures
        """
        try:
            
            funding_rates = self._rate_limited_funding_rates(
                symbol=symbol,
                startTime=start_time,
                endTime=end_time,
                limit=limit
            )
            
            if not funding_rates:
                return pd.DataFrame()
            
            df = pd.DataFrame(funding_rates)           
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching funding rate data for {symbol}: {e}")
            raise 
    
    def get_historical_funding_rate(self,
                              symbol: str,
                              start_time: Union[str, datetime, int],
                              end_time: Union[str, datetime, int],
                              delivery_date: Union[str, datetime, int] = '2100-12-25 08:00:00',
                              limit: int = 1000) -> pd.DataFrame:
        """
        Fetch historical funding rate data by making multiple requests if needed
        Handles rate limiting and pagination
        
        Parameters:
        - symbol: Trading pair symbol
        - start_time: Start time for historical data
        - end_time: End time for historical data
        - delivery_date: Delivery date for perpetual futures
        - limit: Number of records per request (max 1000)
        
        Returns:
        - DataFrame with funding rate history
        """
        try:
            all_funding_rates = []
            current_start = start_time
            
            # Convert times to milliseconds timestamp
            if isinstance(start_time, (str, datetime)):
                current_start = int(pd.Timestamp(start_time).timestamp() * 1000)
            if isinstance(end_time, (str, datetime)):
                end_time = int(pd.Timestamp(end_time).timestamp() * 1000)
                
            # Get and check delivery date
            #with connect_duckdb(self.db_path) as con:
                #delivery_date = con.execute(f'''select delivery_date from bn_perp_symbols where symbol = '{symbol}' ''').fetchone()[0]
            
            if delivery_date and isinstance(delivery_date, (str, datetime)):
                # Convert delivery_date to UTC timestamp in milliseconds
                delivery_date_ms = int(pd.Timestamp(delivery_date).timestamp() * 1000)
                
                # Compare millisecond timestamps directly
                if delivery_date_ms < end_time:
                    end_time = delivery_date_ms
            
            while True:
                if current_start > end_time:
                    break
                
                # time.sleep(self.PAGINATION_DELAY)
                df = self.get_funding_rate(
                    symbol=symbol,
                    start_time=current_start,
                    end_time=end_time,
                    limit=limit
                )
                
                if df is None or df.empty:
                    break
                    
                all_funding_rates.append(df)
                
                # Update start_time for next request
                #current_start = int(df['fundingTime'].iloc[-1].timestamp() * 1000 + 1)
                current_start = int(df['fundingTime'].iloc[-1]) + 1
            
            if all_funding_rates:
                result = pd.concat(all_funding_rates, axis=0)
                result = (result
                        .sort_values(['symbol', 'fundingTime'])
                        .drop_duplicates(subset=['symbol', 'fundingTime'], keep='last')
                        .reset_index(drop=True)
                        )
                
                return result
                
            return pd.DataFrame()
            
        except Exception as e:
            self.logger.error(f"Error fetching historical funding rate data for {symbol}: {e}")
            raise
    
    def fetch_market_funding_rates(self,
                                start_time: Union[str, datetime, int],
                                end_time: Union[str, datetime, int],
                                batch_size: int = None) -> pd.DataFrame:
        """Fetch funding rates with batching and rate limiting"""
        try:
            # Get current perpetual symbols

            symbols_df = clickhouse_query(self.con, f'''
                    select symbol, delivery_date 
                    from bn_perp_symbols 
                    where delivery_date is not null''')
            
            # Force reasonable batch size
            batch_size = batch_size or self.BATCH_SIZE
            num_batches = math.ceil(len(symbols_df) / batch_size)
            
            self.logger.info(
                f"Fetching funding rates for {len(symbols_df)} perpetual symbols "
                f"in {num_batches} batches"
            )

            # Time range setting
            if end_time is None:
                end_time = datetime.now(timezone.utc)
            if start_time is None:
                start_time = datetime(2024, 1, 20)

            # Process in batches
            all_rates = []
            with tqdm(total=len(symbols_df), desc="Fetching funding rates") as pbar:
                for i in range(0, len(symbols_df), batch_size):
                    batch_symbols = symbols_df.iloc[i:i + batch_size, 0]
                    batch_delivery_dates = symbols_df.iloc[i:i + batch_size, 1]
                    batch_rates = []
                    
                    for symbol, delivery_date in zip(batch_symbols, batch_delivery_dates):
                        try:
                            # Add delay between symbols
                            #time.sleep(self.SYMBOL_DELAY)  # 150ms delay
                            
                            rates = self.get_historical_funding_rate(
                                symbol=symbol,
                                start_time=start_time,
                                end_time=end_time,
                                delivery_date=delivery_date
                            )
                            
                            if rates is not None and not rates.empty:
                                batch_rates.append(rates)
                                self.logger.info(
                                    f"Fetched {len(rates)} funding rates for {symbol} "
                                    f"from {pd.to_datetime(rates['fundingTime'].min(), unit='ms')} to {pd.to_datetime(rates['fundingTime'].max(), unit='ms')}"
                                )
                        
                        except Exception as e:
                            self.logger.error(f"Error fetching funding rates for {symbol}: {e}")
                        finally:
                            pbar.update(1)
                    
                    if batch_rates:
                        # Process batch results
                        batch_result = pd.concat(batch_rates, axis=0)
                        batch_result = (batch_result
                                    .drop_duplicates(subset=['symbol', 'fundingTime'], keep='last')
                                    .sort_values(['symbol', 'fundingTime'])
                                    )                                   

                        all_rates.append(batch_result)
                    
                    # Clear memory
                    del batch_rates
                    if 'batch_result' in locals():
                        del batch_result
                    
                    # Add delay between batches
                    #time.sleep(self.BATCH_DELAY)  # 500ms delay
            
            if all_rates:
                # Final concatenation and cleanup
                result = pd.concat(all_rates, axis=0)
                result = (result
                        .drop_duplicates(subset=['symbol', 'fundingTime'], keep='last')
                        .sort_values(['symbol', 'fundingTime'])
                        .reset_index(drop=True))
                
                self.logger.info(
                    f"Successfully fetched {len(result)} funding rates "
                    f"for {len(result['symbol'].unique())} symbols"
                )

                if not result.empty:
                    result['fundingTime'] = pd.to_datetime(result['fundingTime'], unit='ms')
                    result[['fundingRate', 'markPrice']] = result[['fundingRate', 'markPrice']].astype(float)
                    result['exchange'] = 'binance'
                    result['type'] = 'PERPETUAL'
                    
                    # Reorder columns
                    columns = ['symbol', 'exchange', 'type', 'fundingTime', 'fundingRate', 'markPrice']
                    return result[columns]
                
            return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"Error in fetch_market_funding_rates: {e}")
            raise
    
    def fetch_market_funding_rates_threadpool(self,
                                start_time: Union[str, datetime, int],
                                end_time: Union[str, datetime, int]) -> pd.DataFrame:
        """Fetch funding rates using thread pool"""
        try:
            # Get current perpetual symbols
            symbols_df = clickhouse_query(self.con, '''
                    select symbol, delivery_date 
                    from bn_perp_symbols 
                    where delivery_date is not null
            ''')

            # Calculate optimal thread count based on rate limit
            # 500 requests per 5 minutes = 100 requests per minute
            # Use conservative number of threads
            max_workers = self.FUTURES_MAX_WORKERS

            self.logger.info(
                f"Fetching funding rates for {len(symbols_df)} perpetual symbols "
                f"using {max_workers} threads"
            )

            # Set default time range if not provided
            if end_time is None:
                end_time = datetime.now(timezone.utc)
            if start_time is None:
                start_time = datetime(2025, 1, 20)

            # Shared result containers with thread safety
            all_results = []
            failed_symbols = []
            result_lock = threading.Lock()

            def process_symbol(symbol: str, delivery_date: str) -> None:
                """Thread worker function"""
                try:
                    rates = self.get_historical_funding_rate(
                        symbol=symbol,
                        start_time=start_time,
                        end_time=end_time,
                        delivery_date=delivery_date
                    )

                    if rates is not None and not rates.empty:
                        with result_lock:
                            all_results.append(rates)
                            self.logger.info(
                                f"Fetched {len(rates)} funding rates for {symbol} "
                                f"from {pd.to_datetime(rates['fundingTime'].min(), unit='ms')} to {pd.to_datetime(rates['fundingTime'].max(), unit='ms')}"
                            )

                except Exception as e:
                    if '418' in str(e):
                        self._handle_rate_limit_error(e)
                        # Retry after IP ban
                        return process_symbol(symbol, delivery_date)
                    with result_lock:
                        failed_symbols.append(symbol)
                        self.logger.error(f"Error fetching funding rates for {symbol}: {e}")

            # Process symbols using thread pool
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []

                # Submit all symbols to thread pool
                for _, row in symbols_df.iterrows():
                    symbol = row['symbol']
                    delivery_date = row['delivery_date']
                    futures.append(
                        executor.submit(process_symbol, symbol, delivery_date)
                    )

                # Show progress bar while waiting for completion
                with tqdm(total=len(futures), desc="Fetching funding rates") as pbar:
                    for future in futures:
                        future.result()  # Wait for completion
                        pbar.update(1)

            # Report failed symbols
            if failed_symbols:
                self.logger.warning(
                    f"Failed to fetch funding rates for {len(failed_symbols)} symbols: {failed_symbols}"
                )

            # Process final results
            if all_results:
                result = pd.concat(all_results, axis=0)
                
                # Handle empty or invalid values
                numeric_cols = ['fundingRate', 'markPrice']
                for col in numeric_cols:
                    # Replace empty strings with NaN
                    result[col] = pd.to_numeric(result[col], errors='coerce')
                    
                    # Optional: fill NaN with 0 or remove rows with NaN
                    # result = result.dropna(subset=[col])  # Remove rows with NaN
                    result[col] = result[col].fillna(0)  # Or fill with 0
                
                result = (result
                        .drop_duplicates(subset=['symbol', 'fundingTime'], keep='last')
                        .sort_values(['symbol', 'fundingTime'])
                        .reset_index(drop=True))

                if not result.empty:
                    result['fundingTime'] = pd.to_datetime(result['fundingTime'], unit='ms')
                    #result[['fundingRate', 'markPrice']] = result[['fundingRate', 'markPrice']].astype(float)
                    result['exchange'] = 'binance'
                    result['type'] = 'PERPETUAL'

                    # Reorder columns
                    columns = ['symbol', 'exchange', 'type', 'fundingTime', 'fundingRate', 'markPrice']
                    return result[columns]

            return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"Error in fetch_market_funding_rates: {e}")
            raise
    
    def _handle_rate_limit_error(self, e: Exception) -> None:
        """Centralized error handling for rate limits and IP bans"""
        error_str = str(e)
        
        # Handle IP ban (418)
        if '418' in error_str:
            ban_time_match = re.search(r'banned until (\d+)', error_str)
            if ban_time_match:
                ban_timestamp = int(ban_time_match.group(1))
                current_timestamp = int(time.time() * 1000)
                wait_time = (ban_timestamp - current_timestamp) / 1000
                
                if wait_time > 0:
                    ban_until = datetime.fromtimestamp(ban_timestamp/1000, tz=timezone.utc)
                    self.logger.warning(
                        f"IP banned until {ban_until}. "
                        f"Waiting {wait_time:.0f} seconds..."
                    )
                    
                    with tqdm(total=int(wait_time), desc="IP Ban Wait", 
                            bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} seconds') as pbar:
                        remaining = wait_time
                        while remaining > 0:
                            sleep_time = min(60, remaining)
                            time.sleep(sleep_time)
                            pbar.update(int(sleep_time))
                            remaining -= sleep_time
                    
                    # Add additional cool-down period after ban expires
                    cool_down = 60  # 60 seconds cool-down
                    self.logger.info(f"IP ban wait completed, cooling down for {cool_down} seconds...")
                    time.sleep(cool_down)
                    return  # 418 is not raised but returned
            
            # Default handling for unparseable ban
            self.logger.warning("Unparseable IP ban, waiting 3 minutes")
            time.sleep(180)  # 3 minutes
            return
            
        # Handle rate limit (429)
        elif '429' in error_str:
            self.logger.warning("Rate limit hit, backing off")
            time.sleep(10)
            raise  # 429 is raised here in the function
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True
    )
    @sleep_and_retry
    @limits(calls=SPOT_WEIGHT_LIMIT/SPOT_KLINE_WEIGHT, period=RATE_LIMIT_PERIOD)  # Conservative spot limit
    def _rate_limited_spot_klines(self, symbol: str, **kwargs) -> List:
        """Rate-limited spot API call"""
        try:
            return self.spot_client.klines(symbol=symbol, **kwargs)
        except Exception as e:
            self._handle_rate_limit_error(e)
            # After waiting for IP ban, retry the request
            if '418' in str(e):
                return self._rate_limited_spot_klines(symbol, **kwargs)  # 418 is not raised but returned
            raise  # other exceptions are raised here

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True
    )
    @sleep_and_retry
    @limits(calls=FUTURES_WEIGHT_LIMIT/FUTURES_KLINE_WEIGHT, period=RATE_LIMIT_PERIOD)  # Conservative futures limit
    def _rate_limited_futures_klines(self, symbol: str, **kwargs) -> List:
        """Rate-limited futures API call"""
        try:
            return self.um_futures_client.klines(symbol=symbol, **kwargs)
        except Exception as e:
            self._handle_rate_limit_error(e)
            # After waiting for IP ban, retry the request
            if '418' in str(e):
                return self._rate_limited_futures_klines(symbol, **kwargs)
            raise
    
    def get_klines(self, 
                   symbol: str,
                   start_time: int = None,
                   end_time: int = None,
                   interval: str = '1m',
                   limit: int = 1000,
                   is_futures: bool = False) -> pd.DataFrame:
        """
        Fetch kline/candlestick data for spot or futures
        
        Parameters:
        - symbol: Trading pair symbol
        - start_time: Start time for historical data; UTC default; [
        - end_time: End time for historical data; UTC default; ]
        - interval: Kline interval ('1m' for 1-minute)
        - limit: Number of records to fetch (max 1000)
        - is_futures: Whether to fetch futures data
        """
        try:
            #client = self.um_futures_client if is_futures else self.spot_client
            api_call = (self._rate_limited_futures_klines if is_futures 
                        else self._rate_limited_spot_klines)
            
            klines = api_call(
                symbol=symbol,
                interval=interval,
                limit=limit,
                startTime=start_time,
                endTime=end_time
            )
            
            if not klines:  # Check for empty response
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 
                'volume', 'close_time', 'quote_volume', 'trades_count',
                'taker_buy_volume', 'taker_buy_quote_volume', 'ignore'
            ])         
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching klines for {symbol}: {e}")
            raise    
    
    def get_historical_klines(self,
                            symbol: str,
                            start_time: Union[str, datetime, int],
                            end_time: Union[str, datetime, int],
                            delivery_date: Union[str, datetime, int] = '2100-12-25 08:00:00',
                            interval: str = '1m',
                            is_futures: bool = False) -> pd.DataFrame:
        """
        Fetch historical kline data by making multiple requests if needed
        Handles rate limiting and pagination
        """
        try:
            all_klines = []
            current_start = start_time
            
            # Convert times to milliseconds timestamp
            if isinstance(start_time, (str, datetime)):
                current_start = int(pd.Timestamp(start_time).timestamp() * 1000)
            if isinstance(end_time, (str, datetime)):
                end_time = int(pd.Timestamp(end_time).timestamp() * 1000)
                
            # Check delivery date for futures
            if is_futures and delivery_date and isinstance(delivery_date, (str, datetime)):
                # get delivery datetime utc; end_time is utc; compare
                #with connect_duckdb(self.db_path) as con:
                    # 取出来就直接是 tz naive; 但实质是 UTC
                    #delivery_date = con.execute(f'''select delivery_date from bn_perp_symbols where symbol = '{symbol}' ''').fetchone()[0]

                # Convert delivery_date to UTC timestamp in milliseconds
                delivery_date_ms = int(pd.Timestamp(delivery_date).timestamp() * 1000)
                
                # Compare millisecond timestamps directly
                if delivery_date_ms < end_time:
                    end_time = delivery_date_ms
            
            while True:
                if current_start > end_time:
                    break
                
                df = self.get_klines(
                    symbol=symbol,
                    start_time=current_start,
                    end_time=end_time,
                    interval=interval,
                    limit=499 if is_futures else 1000,  # limit parameter ---- rate limit
                    is_futures=is_futures
                )
                
                if df is None or df.empty:
                    break
                    
                all_klines.append(df)
                
                # Update start_time for next request
                current_start = int(df['timestamp'].iloc[-1]) + 1
            
            if all_klines:
                result = pd.concat(all_klines, axis=0)
                
                result = (result
                        .drop_duplicates(subset=['timestamp'], keep='last')
                        .sort_values(['timestamp'])
                        .reset_index(drop=True)
                        )
                
                result['symbol'] = symbol
                return result
            
            return pd.DataFrame()
            
        except Exception as e:
            self.logger.error(f"Error fetching historical kline data for {symbol}: {e}")
            raise
    
    def fetch_market_klines(self,
                        start_time: Union[str, datetime, int],
                        end_time: Union[str, datetime, int],
                        interval: str = '1m',
                        is_futures: bool = False,
                        batch_size: int = None) -> pd.DataFrame:
        """
        Fetch historical kline data for all symbols with batching and rate limiting
        """
        try:
            # Get current active symbols
            if is_futures:
                symbols_df = clickhouse_query(self.con, f'''
                        select symbol, delivery_date 
                        from bn_perp_symbols 
                        where delivery_date is not null
                ''')
            else:
                symbols_df = clickhouse_query(self.con, f'''
                        select symbol 
                        from bn_spot_symbols 
                        where quote_asset in ('USDT') 
                ''')
            
            batch_size = batch_size or self.BATCH_SIZE
            num_batches = math.ceil(len(symbols_df) / batch_size)
            
            self.logger.info(
                f"Fetching {interval} klines for {len(symbols_df)} "
                f"{'perpetual' if is_futures else 'spot'} symbols in {num_batches} batches"
            )

            # Set default time range if not provided
            if end_time is None:
                end_time = datetime.now(timezone.utc)
            if start_time is None:
                start_time = datetime(2025, 1, 20)

            # Process symbols in batches
            all_results = []
            failed_symbols = []
            with tqdm(total=len(symbols_df), desc="Fetching klines") as pbar:
                for i in range(0, len(symbols_df), batch_size):
                    try:
                        batch_symbols = symbols_df.iloc[i:i + batch_size, 0]
                        batch_delivery_dates = symbols_df.iloc[i:i + batch_size, 1] if is_futures else [None] * batch_size
                        batch_klines = []
                        
                        # Process each symbol in the batch
                        for symbol, delivery_date in zip(batch_symbols, batch_delivery_dates):
                            try:
                                # Add delay between symbols within batch
                                #time.sleep(self.SYMBOL_DELAY)  # 50ms delay between symbols
                                
                                klines = self.get_historical_klines(
                                    symbol=symbol,
                                    start_time=start_time,
                                    end_time=end_time,
                                    delivery_date=delivery_date,
                                    interval=interval,
                                    is_futures=is_futures
                                )
                                
                                if klines is not None and not klines.empty:
                                    batch_klines.append(klines)
                                    self.logger.info(
                                        f"Fetched {len(klines)} klines for {symbol} "
                                        f"from {klines['timestamp'].min()} to {klines['timestamp'].max()}"
                                    )
                            
                            except Exception as e:
                                if '418' in str(e):
                                    self._handle_rate_limit_error(e)
                                    i -= batch_size  # Retries the entire batch
                                    break
                                failed_symbols.append(symbol)
                                self.logger.error(f"Error fetching {symbol}: {e}")
                            finally:
                                pbar.update(1)
                        
                        if batch_klines:
                            # Process batch results
                            batch_result = pd.concat(batch_klines, axis=0)
                            batch_result = (batch_result
                                        .drop_duplicates(subset=['symbol', 'timestamp'], keep='last')
                                        .sort_values(['symbol', 'timestamp']))
                            all_results.append(batch_result)
                        
                        # Clear memory
                        del batch_klines
                        if 'batch_result' in locals():
                            del batch_result
                        
                        # Add delay between batches
                        #time.sleep(self.BATCH_DELAY)  # 500ms delay between batches
                    
                    except Exception as e:
                        self.logger.error(f"Error processing batch: {e}")
                        time.sleep(5)

            # Report failed symbols
            if failed_symbols:
                self.logger.warning(f"Failed to fetch data for {len(failed_symbols)} symbols: {failed_symbols}")
            
            if all_results:
                # Final concatenation and cleanup
                result = pd.concat(all_results, axis=0)
                result = (result
                        .sort_values(['symbol', 'timestamp'])
                        .drop_duplicates(subset=['symbol', 'timestamp'], keep='last')                      
                        .reset_index(drop=True))
                
                self.logger.info(
                        f"Successfully fetched {len(result)} klines "
                        f"for {len(result['symbol'].unique())} symbols"
                )

                if not result.empty:
                    # Convert types
                    result['timestamp'] = pd.to_datetime(result['timestamp'], unit='ms')
                    result['close_time'] = pd.to_datetime(result['close_time'], unit='ms')
                    
                    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 
                                    'quote_volume', 'taker_buy_volume', 'taker_buy_quote_volume', 'ignore']
                    result[numeric_cols] = result[numeric_cols].astype(float)
                    
                    
                    result['exchange'] = 'binance'
                    result['type'] = 'SPOT' if not is_futures else 'PERPETUAL'
                    result['interval'] = interval
                
                    columns = ['symbol', 'exchange', 'type', 'interval', 'timestamp', 'close_time', 
                               'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'taker_buy_volume', 'taker_buy_quote_volume', 'trades_count', 'ignore']
                    return result[columns]
                                   
            return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"Error in fetch_market_klines: {e}")
            raise 

    def fetch_market_klines_threadpool(self,
                        start_time: Union[str, datetime, int],
                        end_time: Union[str, datetime, int],
                        interval: str = '1m',
                        is_futures: bool = False) -> pd.DataFrame:
        """Fetch historical kline data using thread pool"""
        try:
            # Get current active symbols
            if is_futures:
                symbols_df = clickhouse_query(self.con, 
                        '''select symbol, delivery_date 
                        from bn_perp_symbols 
                        where delivery_date is not null ''')
            else:
                symbols_df = clickhouse_query(self.con, 
                        '''select symbol 
                        from bn_spot_symbols 
                        where quote_asset in ('USDT') ''')

            # Calculate optimal thread count based on rate limits
            if is_futures:
                # For futures with limit=1000: 2400/5 = 480 requests/min
                max_workers = self.FUTURES_MAX_WORKERS  # Conservative
            else:
                max_workers = self.SPOT_MAX_WORKERS  # Spot has higher limit

            self.logger.info(
                f"Fetching {interval} klines for {len(symbols_df)} "
                f"{'perpetual' if is_futures else 'spot'} symbols using {max_workers} threads"
            )

            # Set default time range if not provided
            if end_time is None:
                end_time = datetime.now(timezone.utc)
            if start_time is None:
                start_time = datetime(2025, 1, 20)
            
            # Shared result containers
            all_results = []
            failed_symbols = []
            result_lock = threading.Lock()
            
            def process_symbol(symbol, delivery_date=None):
                """Thread worker function"""
                try:
                    klines = self.get_historical_klines(
                        symbol=symbol,
                        start_time=start_time,
                        end_time=end_time,
                        delivery_date=delivery_date,
                        interval=interval,
                        is_futures=is_futures
                    )
                    
                    if klines is not None and not klines.empty:
                        with result_lock:
                            all_results.append(klines)
                            self.logger.info(
                                f"Fetched {len(klines)} klines for {symbol} "
                                f"from {pd.to_datetime(klines['timestamp'].min(), unit='ms')} to {pd.to_datetime(klines['timestamp'].max(), unit='ms')}"
                            )
                    
                except Exception as e:
                    if '418' in str(e):
                        self._handle_rate_limit_error(e)
                        # Retry after IP ban
                        return process_symbol(symbol, delivery_date)
                    with result_lock:
                        failed_symbols.append(symbol)
                        self.logger.error(f"Error fetching {symbol}: {e}")

            # Process symbols using thread pool
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                
                # Submit all symbols to thread pool
                for _, row in symbols_df.iterrows():
                    symbol = row['symbol']
                    delivery_date = row['delivery_date'] if is_futures else None
                    futures.append(
                        executor.submit(process_symbol, symbol, delivery_date)
                    )
                
                # Show progress bar while waiting for completion
                with tqdm(total=len(futures), desc="Fetching klines") as pbar:
                    for future in futures:
                        future.result()  # Wait for completion
                        pbar.update(1)

            # Report failed symbols
            if failed_symbols:
                self.logger.warning(
                    f"Failed to fetch data for {len(failed_symbols)} symbols: {failed_symbols}"
                )

            # Process final results
            if all_results:
                result = pd.concat(all_results, axis=0)
                result = (result
                        .sort_values(['symbol', 'timestamp'])
                        .drop_duplicates(subset=['symbol', 'timestamp'], keep='last')
                        .reset_index(drop=True))

                if not result.empty:
                    # Convert types
                    result['timestamp'] = pd.to_datetime(result['timestamp'], unit='ms')
                    result['close_time'] = pd.to_datetime(result['close_time'], unit='ms')
                    
                    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 
                                'quote_volume', 'taker_buy_volume', 
                                'taker_buy_quote_volume', 'ignore']
                    result[numeric_cols] = result[numeric_cols].astype(float)
                    
                    result['exchange'] = 'binance'
                    result['type'] = 'SPOT' if not is_futures else 'PERPETUAL'
                    result['interval'] = interval

                    columns = ['symbol', 'exchange', 'type', 'interval', 'timestamp', 
                            'close_time', 'open', 'high', 'low', 'close', 'volume', 
                            'quote_volume', 'taker_buy_volume', 'taker_buy_quote_volume', 
                            'trades_count', 'ignore']
                    return result[columns]

            return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"Error in fetch_market_klines: {e}")
            raise

    
    def calculate_premium_wma(self,
                            start_time: Union[str, datetime, int],
                            end_time: Union[str, datetime, int]) -> pd.DataFrame:
        """Calculate premium WMA with batching and rate limiting"""
        try:
            # Get current perpetual symbols
            window = 120
            q = f'''
            SELECT 
                p.symbol,
                p.exchange,
                p.timestamp,
                p.close_time,
                p.close / s.close - 1 as premium
            FROM bn_perp_klines p
            INNER JOIN bn_spot_klines s
                ON p.symbol = s.symbol
                AND p.timestamp = s.timestamp
            WHERE p.timestamp >= TIMESTAMP '{start_time}' - interval '{window} minute'
                AND p.timestamp <= TIMESTAMP '{end_time}'
            ORDER BY p.symbol, p.timestamp
            '''
            df = clickhouse_query(self.con, q)

            def calculate_wma(df, field='premium', window=120):
                """
                Calculate WMA of premium over specified hours
                
                Parameters:
                df: DataFrame with 'timestamp', 'symbol', field='ps_premium'
                hours: lookback window in hours
                """
                
                # Method 1: Using pandas_ta (simplest)
                df[f'wma{window}_{field}'] = df.groupby('symbol')[field].transform(
                    lambda x: ta.wma(x, length=window)
                )
                
                return df
            
            premium_wma = calculate_wma(df, field='premium', window=window)
        
            return premium_wma[premium_wma['timestamp'].between(start_time, end_time)]

        except Exception as e:
            self.logger.error(f"Error in calculate_premium_wma: {e}")
            raise

@dataclass
class TableConfig:
    """Configuration for a market data table"""
    name: str                                    # Table name
    primary_keys: List[str]                      # Primary key columns
    columns: Dict[str, str]                      # Column names and types
    fetch_func: Callable                         # Function to fetch data
    fetch_args: Dict[str, Any]                   # Additional arguments for fetch_func
    update_frequency: str                        # Update frequency ('minute', 'hour', '8h', 'day')
    needs_incremental: bool = True               # Whether table needs incremental updates


def create_crypto_data_pipeline(con: Client, klines_interval: str = '1h') -> Dict[str, TableConfig]:
    """Create configurations for different crypto market data tables"""
    
    # Define table configurations
    table_configs = {
        # Binance Spot Symbols
        'bn_spot_symbols': TableConfig(
            name='bn_spot_symbols',
            primary_keys=['symbol', 'exchange'],
            columns={
                'symbol': 'String',
                'base_asset': 'String',
                'quote_asset': 'String',
                'exchange': 'String',
                'type': 'String',
                'status': 'String',
                'is_spot_trading_allowed': 'UInt8',
                'is_margin_trading_allowed': 'UInt8',
                'base_precision': 'Int32',
                'quote_precision': 'Int32',
                'min_price': 'Float64',
                'max_price': 'Float64',
                'tick_size': 'Float64',
                'min_qty': 'Float64',
                'max_qty': 'Float64',
                'step_size': 'Float64'
            },
            fetch_func=lambda fetcher: fetcher.get_spot_symbols(),
            fetch_args={},
            update_frequency='daily',
            needs_incremental=False
        ),
        
        # Binance Perpetual Futures Symbols
        'bn_perp_symbols': TableConfig(
            name='bn_perp_symbols',
            primary_keys=['symbol', 'exchange'],
            columns={
                'symbol': 'String',
                'base_asset': 'String',
                'quote_asset': 'String',
                'margin_asset': 'String',
                'exchange': 'String',
                'type': 'String',
                'underlyingSubType': 'String',
                'status': 'String',
                'onboard_date': 'DateTime',
                'delivery_date': 'DateTime',
                'price_precision': 'Int32',
                'quantity_precision': 'Int32',
                'min_price': 'Float64',
                'max_price': 'Float64',
                'tick_size': 'Float64',
                'min_qty': 'Float64',
                'max_qty': 'Float64',
                'step_size': 'Float64'
            },
            fetch_func=lambda fetcher: fetcher.get_um_perpetual_symbols(),
            fetch_args={},
            update_frequency='daily',
            needs_incremental=False
        ),
        
        # Binance Spot Klines
        'bn_spot_klines': TableConfig(
            name='bn_spot_klines',
            primary_keys=['symbol', 'exchange', 'interval', 'timestamp'],
            columns={
                'symbol': 'String',
                'exchange': 'String',
                'type': 'String',
                'interval': 'String',
                'timestamp': 'DateTime',
                'close_time': 'DateTime',
                'open': 'Float64',
                'high': 'Float64',
                'low': 'Float64',
                'close': 'Float64',
                'volume': 'Float64',
                'quote_volume': 'Float64',
                'taker_buy_volume': 'Float64',
                'taker_buy_quote_volume': 'Float64',
                'trades_count': 'Int32'
            },
            fetch_func=lambda fetcher, start_time, end_time: 
                fetcher.fetch_market_klines_threadpool(
                    start_time=start_time,
                    end_time=end_time,
                    interval=klines_interval,
                    is_futures=False
                ),
            fetch_args={},
            update_frequency=klines_interval,
            needs_incremental=True
        ),
        
        # Binance Perpetual Klines
        'bn_perp_klines': TableConfig(
            name='bn_perp_klines',
            primary_keys=['symbol', 'exchange', 'interval', 'timestamp'],
            columns={
                'symbol': 'String',
                'exchange': 'String',
                'type': 'String',
                'interval': 'String',
                'timestamp': 'DateTime',
                'close_time': 'DateTime',
                'open': 'Float64',
                'high': 'Float64',
                'low': 'Float64',
                'close': 'Float64',
                'volume': 'Float64',
                'quote_volume': 'Float64',
                'taker_buy_volume': 'Float64',
                'taker_buy_quote_volume': 'Float64',
                'trades_count': 'Int32'
            },
            fetch_func=lambda fetcher, start_time, end_time: 
                fetcher.fetch_market_klines_threadpool(
                    start_time=start_time,
                    end_time=end_time,
                    interval=klines_interval,
                    is_futures=True
                ),
            fetch_args={},
            update_frequency=klines_interval,
            needs_incremental=True
        ),

        # Binance Premium; Calculation 
        'bn_premium': TableConfig(
            name='bn_premium',
            primary_keys=['symbol', 'exchange', 'timestamp'],
            columns={
                'symbol': 'String',
                'exchange': 'String',
                'timestamp': 'DateTime',
                'close_time': 'DateTime',
                'premium': 'Float64',
                'wma120_premium': 'Float64' 
            },
            fetch_func=lambda fetcher, start_time, end_time: 
                fetcher.calculate_premium_wma(
                    start_time=start_time,
                    end_time=end_time
                ),
            fetch_args={},
            update_frequency=klines_interval,  # Funding rates update every 8 hours
            needs_incremental=True
        ),
        
        # Binance Funding Rates; UM Perpetual Futures
        'bn_funding_rates': TableConfig(
            name='bn_funding_rates',
            primary_keys=['symbol', 'exchange', 'fundingTime'],
            columns={
                'symbol': 'String',
                'exchange': 'String',
                'type': 'String',
                'fundingTime': 'DateTime',
                'fundingRate': 'Float64',
                'markPrice': 'Float64'
            },
            fetch_func=lambda fetcher, start_time, end_time: 
                fetcher.fetch_market_funding_rates_threadpool(
                    start_time=start_time,
                    end_time=end_time
                ),
            fetch_args={},
            update_frequency='2h',  # Funding rates update every 8 hours
            needs_incremental=True
        ),

        # Binance Margin Interest Rates; Spot
        'bn_margin_interest_rates': TableConfig(
            name='bn_margin_interest_rates',
            primary_keys=['asset', 'exchange', 'timestamp'],
            columns={
                'asset': 'String',
                'exchange': 'String',
                'type': 'String',
                'timestamp': 'DateTime',
                'dailyInterestRate': 'Float64',
                'vipLevel': 'Int32'
            },
            fetch_func=lambda fetcher, start_time, end_time: 
                fetcher.fetch_market_margin_interest_rates_threadpool(
                    start_time=start_time,
                    end_time=end_time,
                    vip_level=0
                ),
            fetch_args={},
            update_frequency='2h',
            needs_incremental=True
        )
    }
    
    return table_configs

class CryptoDataPipeline:
    def __init__(self, con: Client, klines_interval: str = '1h', bn_api_key: str = None, bn_api_secret: str = None):
        self.con = con
        self.klines_interval = klines_interval
        self.fetcher = BinanceDataFetcher(con, api_key=bn_api_key, api_secret=bn_api_secret)
        self.table_configs = create_crypto_data_pipeline(con, klines_interval)
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Initialize database and tables
        self._initialize_database()
        
    def _initialize_database(self):
        """Initialize database tables if they don't exist"""
        try:

            for config in self.table_configs.values():
                columns_sql = ', '.join(f"{col} {dtype}" for col, dtype in config.columns.items())
                primary_key_sql = ', '.join(config.primary_keys)
                
                create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {config.name} (
                        {columns_sql}
                    ) ENGINE = ReplacingMergeTree()
                    PRIMARY KEY ({primary_key_sql})
                    ORDER BY ({primary_key_sql})
                """
                self.logger.info(f"Creating table {config.name} if not exists")
                self.con.execute(create_table_sql)
        
        except Exception as e:
            print(f"Error initializing database: {e}")
            raise
    
    def get_latest_update(self, config: TableConfig) -> Optional[datetime]:
        """Get the latest timestamp in a table"""
        try:
            # Find the appropriate time column
            time_col = next((col for col in ['timestamp', 'fundingTime'] 
                            if col in config.columns), None)
            
            if not time_col:
                print(f"No time column found for {config.name}")
                return None
            
            result = self.con.execute(f"SELECT MAX({time_col}) FROM {config.name}")[0][0]
            #print(result)
            if result:
                # UTC
                return pd.Timestamp(result) #.tz_localize('Asia/Shanghai').tz_convert('UTC')
            else:
                return None
        
        except Exception as e:
            self.logger.error(f"Error getting latest timestamp for {config.name}: {e}")
            return None
    
    def update_table(self, config: TableConfig, new_df: pd.DataFrame):
        """Update table with new data using ReplacingMergeTree"""
        if len(new_df) == 0:
            return
            
        start_time = time.time()
        
        try:
            # Ensure all columns exist and are in correct order
            for col in config.columns.keys():
                if col not in new_df.columns:
                    new_df[col] = None
            new_df = new_df[config.columns.keys()]
                        
            # Convert boolean columns to UInt8
            bool_columns = [col for col, dtype in config.columns.items() 
                        if dtype == 'UInt8']
            for col in bool_columns:
                if col in new_df.columns:
                    # Explicitly convert to int using numpy
                    new_df.loc[:, col] = np.where(new_df[col], 1, 0).astype(np.uint8)
            
            # Insert new data (ReplacingMergeTree will handle duplicates)
            self.con.insert_dataframe(
                f"INSERT INTO {config.name} VALUES",
                new_df
            )
            
            # Optionally force merge to remove old versions
            # Note: This is resource-intensive, might want to do it less frequently
            self.con.execute(f"OPTIMIZE TABLE {config.name} FINAL")
            
            print(f"Time taken to update {config.name}: {time.time() - start_time:.2f} seconds")
            
        except Exception as e:
            self.logger.error(f"Error updating table {config.name}: {e}")
            raise

    def update_market_data(self, 
                        config: TableConfig,
                        start_time: Optional[datetime] = None,
                        end_time: Optional[datetime] = None):
        """
        Update market data for a specific table
        
        Parameters:
        - config: 
        - start_time: Optional start time for incremental tables
        - end_time: Optional end time for incremental tables
        """
        try:
            start = time.time()
            # Handle different update patterns based on needs_incremental
            if config.needs_incremental:
                # For incremental tables (klines and funding rates)
                # Set default end time to now if not provided
                if end_time is None:
                    end_time = pd.Timestamp.now(tz='UTC').tz_localize(None)
                
                if start_time is None:
                    # Get latest timestamp if not provided
                    latest_time = self.get_latest_update(config) # tz naive; UTC
                    if latest_time:
                        # Add a small buffer to avoid gaps
                        if config.update_frequency == '1h':
                            start_time = latest_time - timedelta(hours=2)
                        elif config.update_frequency == '2h':
                            start_time = latest_time - timedelta(hours=8)
                    else:
                        # If no data exists, use a default start time
                        start_time = pd.Timestamp(datetime(2020, 1, 1)) 
                    print(f"Start time: {start_time}")                                    
                
                # Fetch new data with time range
                print(f"\n=== Fetching {config.name} data from {start_time} to {end_time} ===")
                new_df = config.fetch_func(
                    self.fetcher,
                    start_time=start_time,
                    end_time=end_time,
                    **config.fetch_args
                )
                
            else:
                # For non-incremental tables (symbol information)
                print(f"\n=== Fetching latest {config.name} data ===")
                new_df = config.fetch_func(
                    self.fetcher,
                    **config.fetch_args
                )

            print(f"Time taken to fetch {config.name}: {time.time() - start:.2f} seconds")
            
            # Update table if we have new data
            if new_df is not None and not new_df.empty:
                print(f"Updating {config.name} with {len(new_df)} rows")           
                #print(new_df[['symbol', 'onboard_date', 'delivery_date']])
                self.update_table(config, new_df)
                print(f"Successfully updated {config.name}")
            else:
                print(f"No new data to update for {config.name}")               
                    
        except Exception as e:
            print(f"Error updating {config.name}: {e}")
            raise

    def update_all(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None):
        """Update all tables in the pipeline"""
        for table_name in ['bn_perp_symbols','bn_spot_symbols']:
            try:
                print(f"\nUpdating {table_name}...")
                self.update_market_data(self.table_configs[table_name], start_time, end_time)
            except Exception as e:
                print(f"Failed to update {table_name}: {e}")
                continue
        
        if end_time is None:
            end_time = pd.Timestamp.now(tz='UTC').tz_localize(None)

        for table_name in ['bn_perp_klines','bn_spot_klines']:
            try:
                print(f"\nUpdating {table_name}...")
                self.update_market_data(self.table_configs[table_name], start_time, end_time)
            except Exception as e:
                print(f"Failed to update {table_name}: {e}")
                continue
        
        for table_name in ['bn_funding_rates']:
            try:
                print(f"\nUpdating {table_name}...")
                self.update_market_data(self.table_configs[table_name], start_time, end_time)
            except Exception as e:
                print(f"Failed to update {table_name}: {e}")
                continue

    def update_timely(self):
        """Update timely data for all tables"""
        start_time = None
        end_time = pd.Timestamp.now(tz='UTC').tz_localize(None)

        for table_name in ['bn_perp_symbols','bn_spot_symbols','bn_perp_klines','bn_spot_klines','bn_premium']:
            try:
                print(f"\n=== Updating {table_name} ===")
                self.update_market_data(self.table_configs[table_name], start_time, end_time)
                print('-'*100)
            except Exception as e:
                print(f"Failed to update {table_name}: {e}")
                continue

    def update_hourly(self):
        """Update hourly data for all tables"""
        start_time = None
        end_time = pd.Timestamp.now(tz='UTC').tz_localize(None)

        for table_name in ['bn_funding_rates']:
            try:
                print(f"\n=== Updating {table_name} ===")
                self.update_market_data(self.table_configs[table_name], start_time, end_time)
                print('-'*100)
            except Exception as e:
                print(f"Failed to update {table_name}: {e}")
                continue
    
    def validate_data(self):
        for type in ['perp', 'spot']:
            print(f"\nValidating {type} klines data...")
            gaps_query = f'''
            WITH time_diffs AS (
                SELECT 
                    symbol,
                    timestamp,
                    anyLast(timestamp) OVER (PARTITION BY symbol ORDER BY timestamp
                        ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) as next_timestamp,
                    dateDiff('hour', timestamp, 
                        anyLast(timestamp) OVER (PARTITION BY symbol ORDER BY timestamp
                            ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
                    ) as hours_diff
                FROM bn_{type}_klines
            )
            SELECT 
                symbol,
                timestamp as gap_start,
                next_timestamp as gap_end,
                hours_diff as gap_hours
            FROM time_diffs
            WHERE hours_diff > 1  -- Gap larger than expected interval
                AND next_timestamp is not null
            ORDER BY gap_hours DESC
            --LIMIT 20  -- Show top 20 largest gaps
            '''
            print("\nChecking for time gaps...")
            gaps_df = clickhouse_query(self.con, gaps_query)
            if not gaps_df.empty:
                print("\nFound time gaps:")
                print(gaps_df)
            else:
                print("No time gaps found.")
    
    def get_extreme_cases(self, interval: int = 30, threshold_delta: float = -0.006, threshold_diff: int = 1440):
        """Get extreme cases for a table"""
        q = f'''
            WITH prepare_fundingRate AS (
                SELECT 
                    p.symbol,
                    p.timestamp AS fundingTime,
                    LAG(p.timestamp, {interval}) OVER (
                        PARTITION BY p.symbol 
                        ORDER BY p.timestamp
                    ) AS prev_fundingTime,
                    wma120_premium as fundingRate,
                    LAG(wma120_premium, {interval}) OVER (
                        PARTITION BY p.symbol 
                        ORDER BY p.timestamp
                    ) AS prev_fundingRate
                FROM bn_premium p
                INNER JOIN bn_perp_symbols s
                    ON p.symbol = s.symbol
                    AND p.timestamp > s.onboard_date + INTERVAL 5 DAY
            ),

            extreme_events AS (
                WITH change AS (
                    SELECT 
                        *,
                        fundingRate - prev_fundingRate as fundingRate_change,
                        DATEDIFF('minute', LAG(fundingTime) OVER (PARTITION BY symbol ORDER BY fundingTime), fundingTime) as fundingTime_diff
                    FROM prepare_fundingRate
                    WHERE fundingRate_change < {threshold_delta}
                )
                SELECT 
                    *
                FROM change
                WHERE (fundingTime_diff is null) OR (fundingTime_diff > {threshold_diff})
            )

            SELECT * FROM extreme_events
            ORDER BY fundingTime DESC
        '''
        extreme_df = clickhouse_query(self.con, q)
        extreme_df['fundingTime_cn'] = extreme_df['fundingTime'].dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai').dt.tz_localize(None)
        
        return extreme_df.head(10)


#from dotenv import load_dotenv
#load_dotenv()

#pipeline = CryptoDataPipeline(con=connect_clickhouse(), klines_interval='1h', bn_api_key=os.environ['BINANCE_API_KEY'], bn_api_secret=os.environ['BINANCE_API_SECRET'])
#print(pipeline.fetcher.get_um_perpetual_symbols())
#pipeline.update_market_data(pipeline.table_configs['bn_spot_symbols'])
#pipeline.update_market_data(pipeline.table_configs['bn_perp_symbols'])
#pipeline.update_market_data(
#    pipeline.table_configs['bn_perp_klines'],
#    start_time=None, #'2025-02-22 07:52:00',
#    end_time='2020-01-03 00:00:00'
#)
#pipeline.validate_data()
#print(pipeline.fetcher.get_margin_interest_rate('BNX', start_time=int(pd.Timestamp('2025-01-20 00:00:00').timestamp()*1000), end_time=int(pd.Timestamp('2025-02-20 00:00:00').timestamp()*1000)))
#print(pipeline.fetcher.get_historical_margin_interest_rate('BNX', '2025-01-20 00:00:00', '2025-03-22 16:30:00', list_date=None, delist_date=None))
#fetcher = BinanceDataFetcher(con=connect_clickhouse(), api_key=os.environ['BINANCE_API_KEY'], api_secret=os.environ['BINANCE_API_SECRET'])
#fetcher.fetch_market_margin_interest_rates_threadpool(start_time=pd.Timestamp('2025-03-28 00:00:00'), end_time=pd.Timestamp.now(tz='UTC'))
#print(fetcher.get_margin_interest_rate('FORM', start_time=int(pd.Timestamp('2025-01-20 00:00:00').timestamp()*1000), end_time=int(pd.Timestamp('2025-02-20 00:00:00').timestamp()*1000)))
#print(fetcher.get_margin_interest_rate('FORM', start_time=int(pd.Timestamp('2025-01-20 00:00:00').timestamp()*1000), end_time=int(pd.Timestamp('2025-04-20 00:00:00').timestamp()*1000)))

#print(fetcher.get_historical_margin_interest_rate('FORM', '2025-02-20 00:00:00', '2025-03-22 16:30:00', list_date='2025-02-20 00:00:00', delist_date='2025-03-22 16:30:00'))
#print(fetcher.get_historical_margin_interest_rate('FORM', '2025-01-20 00:00:00', '2025-03-22 16:30:00', list_date=None, delist_date=None))

#print(fetcher.get_funding_rate('FORM', int(pd.Timestamp('2025-01-20 00:00:00').timestamp()*1000), int(pd.Timestamp('2025-02-20 00:00:00').timestamp()*1000)))
#con = connect_clickhouse()
#con.disconnect()
