import os
import requests
import json
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List
from abc import ABC, abstractmethod
from .kafka_config import KafkaConfig
from .utils import ErrorHandler, TradingSchedule
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataExtractor(ABC):
    @abstractmethod
    def extract(self) -> pd.DataFrame:
        pass

    def save_raw_data(self, data: Dict[str, Any], filename: str) -> None:
        os.makedirs('./data/raw', exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filepath = f'./data/raw/{filename}_{timestamp}.json'
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)

class AlphaVantageExtractor(DataExtractor):
    def __init__(self, api_key: str, kafka_config: KafkaConfig = None):
        self.api_key = api_key
        self.base_url = 'https://www.alphavantage.co/query'
        self.symbols = ['NVDA', 'AAPL', 'MSFT', 'AVGO','CRM','PLTR']  # Example symbols
        self.kafka_config = kafka_config or KafkaConfig()
        
        # Initialize rate limiter and trading schedule
        self.rate_limiter = RateLimiter()
        self.trading_schedule = TradingSchedule()
    
    def _make_request(self, function: str, params: Dict[str, str]) -> Dict:
        """Make API request with trading-aware rate limiting"""
        # Check if we can make a request
        if not self.rate_limiter.can_make_request():
            next_request_time = self.trading_schedule.get_next_request_time()
            wait_time = (next_request_time - datetime.now()).total_seconds()
            if wait_time > 0:
                logger.info(f"Waiting {wait_time:.0f} seconds until next request")
                time.sleep(wait_time)
        
        try:
            params['apikey'] = self.api_key
            response = requests.get(self.base_url, params=params)
            
            if response.status_code != 200:
                raise Exception(f"API request failed: {response.text}")
            
            data = response.json()
            if 'Error Message' in data:
                raise Exception(f"API error: {data['Error Message']}")
            
            # Record the request
            self.rate_limiter.add_request()
            
            return data
            
        except Exception as e:
            ErrorHandler.handle_error(e, {
                'function': function,
                'params': {k: v for k, v in params.items() if k != 'apikey'}
            })
            raise

    def _publish_to_kafka(self, topic: str, symbol: str, data: Dict[str, Any]):
        """Publish data to Kafka"""
        try:
            message = {
                'timestamp': datetime.now().isoformat(),
                'symbol': symbol,
                'data': data
            }
            
            self.kafka_config.produce(
                topic=topic,
                key=symbol,
                value=message
            )
        except Exception as e:
            ErrorHandler.handle_error(e, {
                'topic': topic,
                'symbol': symbol
            })

    def get_intraday_data(self, symbol: str, interval: str = '5min') -> pd.DataFrame:
        """Get real-time intraday data"""
        params = {
            'function': 'TIME_SERIES_INTRADAY',
            'symbol': symbol,
            'interval': interval,
            'outputsize': 'full'
        }
        data = self._make_request('TIME_SERIES_INTRADAY', params)
        time_series = data.get(f'Time Series ({interval})', {})
        
        # Publish raw data to Kafka
        self._publish_to_kafka(
            self.kafka_config.TOPICS['raw_data'],
            symbol,
            {'intraday': data}
        )
        
        df = pd.DataFrame.from_dict(time_series, orient='index')
        df.index = pd.to_datetime(df.index)
        df.columns = ['open', 'high', 'low', 'close', 'volume']
        
        # Publish processed data to Kafka
        self._publish_to_kafka(
            self.kafka_config.TOPICS['intraday'],
            symbol,
            df.to_dict(orient='records')
        )
        
        return df

    def get_market_sentiment(self, symbol: str) -> pd.DataFrame:
        """Get news sentiment data"""
        params = {
            'function': 'NEWS_SENTIMENT',
            'tickers': symbol
        }
        data = self._make_request('NEWS_SENTIMENT', params)
        feed = data.get('feed', [])
        
        # Publish sentiment data to Kafka
        self._publish_to_kafka(
            self.kafka_config.TOPICS['sentiment'],
            symbol,
            {'sentiment': feed}
        )
        
        return pd.DataFrame(feed)

    def get_technical_indicators(self, symbol: str) -> Dict[str, pd.DataFrame]:
        """Get various technical indicators"""
        indicators = {}
        
        # RSI
        rsi_params = {
            'function': 'RSI',
            'symbol': symbol,
            'interval': 'daily',
            'time_period': '14',
            'series_type': 'close'
        }
        rsi_data = self._make_request('RSI', rsi_params)
        indicators['rsi'] = pd.DataFrame(rsi_data.get('Technical Analysis: RSI', {})).T

        # MACD
        macd_params = {
            'function': 'MACD',
            'symbol': symbol,
            'interval': 'daily',
            'series_type': 'close'
        }
        macd_data = self._make_request('MACD', macd_params)
        indicators['macd'] = pd.DataFrame(macd_data.get('Technical Analysis: MACD', {})).T

        # Publish indicators to Kafka
        self._publish_to_kafka(
            self.kafka_config.TOPICS['indicators'],
            symbol,
            {
                'rsi': indicators['rsi'].to_dict(orient='records'),
                'macd': indicators['macd'].to_dict(orient='records')
            }
        )

        return indicators

    def extract(self) -> Dict[str, pd.DataFrame]:
        """Extract comprehensive market data"""
        if not self.trading_schedule.is_market_open():
            logger.info("Market is currently closed. No data will be collected.")
            return {}
            
        all_data = {}
        
        for symbol in self.symbols:
            try:
                symbol_data = {}
                
                # Get real-time intraday data
                symbol_data['intraday'] = self.get_intraday_data(symbol)
                
                # Get market sentiment
                symbol_data['sentiment'] = self.get_market_sentiment(symbol)
                
                # Get technical indicators
                symbol_data['indicators'] = self.get_technical_indicators(symbol)
                
                all_data[symbol] = symbol_data
                
                # Save raw data
                self.save_raw_data(symbol_data, f'alphavantage_{symbol}')
                
            except Exception as e:
                ErrorHandler.handle_error(e, {'symbol': symbol})
                continue
        
        # Ensure all messages are delivered
        self.kafka_config.flush()
        
        return all_data

def get_extractor(source: str = 'alphavantage', **kwargs) -> DataExtractor:
    """Factory function to get the appropriate extractor"""
    if source.lower() == 'alphavantage':
        return AlphaVantageExtractor(
            api_key=kwargs.get('api_key'),
            kafka_config=kwargs.get('kafka_config')
        )
    else:
        raise ValueError(f"Unknown source: {source}")