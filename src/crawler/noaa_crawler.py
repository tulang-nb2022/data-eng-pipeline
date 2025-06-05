import os
import json
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import requests
from kafka import KafkaProducer
import pandas as pd
import numpy as np
from scipy import stats
import logging
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class WeatherMetrics:
    """Class for storing weather metrics with validation ranges"""
    temperature: float  # Celsius
    precipitation: float  # mm
    wind_speed: float  # m/s
    humidity: float  # percentage
    pressure: float  # hPa
    visibility: float  # km
    
    @classmethod
    def get_valid_ranges(cls) -> Dict[str, tuple]:
        return {
            'temperature': (-90, 60),  # Celsius
            'precipitation': (0, 1000),  # mm
            'wind_speed': (0, 100),  # m/s
            'humidity': (0, 100),  # percentage
            'pressure': (800, 1100),  # hPa
            'visibility': (0, 50)  # km
        }

class NOAACrawler:
    def __init__(self, api_key: str, config_path: str = "config/noaa_config.yml"):
        self.api_key = api_key
        self.config = self._load_config(config_path)
        self.base_url = self.config['noaa']['api']['base_url']
        self.producer = self._create_kafka_producer()
        self.session = requests.Session()
        self.session.headers.update({'token': api_key})
        
    def _load_config(self, config_path: str) -> Dict:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _create_kafka_producer(self) -> KafkaProducer:
        kafka_config = self.config['noaa']['kafka']
        return KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
    
    def fetch_station_metadata(self, station_id: str) -> Dict:
        """Fetch detailed metadata for a weather station"""
        url = f"{self.base_url}/stations/{station_id}"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()
    
    def fetch_historical_data(
        self,
        start_date: datetime,
        end_date: datetime,
        station_id: str,
        datatypes: Optional[List[str]] = None
    ) -> List[Dict]:
        """Fetch historical weather data with rate limiting"""
        if datatypes is None:
            datatypes = self.config['noaa']['data']['default_datatypes']
        
        all_data = []
        current_date = start_date
        
        while current_date <= end_date:
            for datatype in datatypes:
                try:
                    data = self._fetch_data_for_date(
                        current_date,
                        station_id,
                        datatype
                    )
                    all_data.extend(data)
                    
                    # Rate limiting
                    time.sleep(1 / self.config['noaa']['api']['rate_limit']['requests_per_second'])
                    
                except Exception as e:
                    logger.error(f"Error fetching data for {current_date}: {str(e)}")
            
            current_date += timedelta(days=1)
        
        return all_data
    
    def _fetch_data_for_date(
        self,
        date: datetime,
        station_id: str,
        datatype: str
    ) -> List[Dict]:
        """Fetch data for a specific date and datatype"""
        url = f"{self.base_url}/data"
        params = {
            'datasetid': self.config['noaa']['data']['default_dataset'],
            'stationid': station_id,
            'datatypeid': datatype,
            'startdate': date.strftime(self.config['noaa']['data']['date_format']),
            'enddate': date.strftime(self.config['noaa']['data']['date_format']),
            'limit': 1000
        }
        
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json().get('results', [])
    
    def send_to_kafka(self, data: Dict) -> None:
        """Send data to Kafka topic"""
        topic = self.config['noaa']['kafka']['topic']
        try:
            self.producer.send(topic, value=data)
            self.producer.flush()
        except Exception as e:
            logger.error(f"Error sending to Kafka: {str(e)}")
    
    def close(self) -> None:
        """Close the Kafka producer"""
        self.producer.close()

class WeatherTransformer:
    def __init__(self):
        self.valid_ranges = WeatherMetrics.get_valid_ranges()
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply weather-specific transformations to the data"""
        # Basic data cleaning and conversion
        df = self._clean_data(df)
        
        # Add derived weather metrics
        df = self._add_derived_metrics(df)
        
        # Add weather patterns and anomalies
        df = self._detect_weather_patterns(df)
        
        # Add climate indices
        df = self._calculate_climate_indices(df)
        
        return df
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate weather data"""
        # Convert units
        df['temperature'] = df['temperature'] / 10.0  # Convert from tenths of Celsius
        df['precipitation'] = df['precipitation'] / 10.0  # Convert from tenths of mm
        
        # Validate ranges
        for col, (min_val, max_val) in self.valid_ranges.items():
            if col in df.columns:
                df = df[df[col].between(min_val, max_val)]
        
        return df
    
    def _add_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived weather metrics"""
        # Heat index calculation
        df['heat_index'] = self._calculate_heat_index(
            df['temperature'],
            df['humidity']
        )
        
        # Wind chill calculation
        df['wind_chill'] = self._calculate_wind_chill(
            df['temperature'],
            df['wind_speed']
        )
        
        # Dew point calculation
        df['dew_point'] = self._calculate_dew_point(
            df['temperature'],
            df['humidity']
        )
        
        # Precipitation intensity
        df['precipitation_intensity'] = df['precipitation'].rolling(
            window=24, min_periods=1
        ).mean()
        
        return df
    
    def _detect_weather_patterns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detect weather patterns and anomalies"""
        # Temperature anomalies
        df['temperature_anomaly'] = df['temperature'] - df['temperature'].rolling(
            window=30, min_periods=1
        ).mean()
        
        # Precipitation anomalies
        df['precipitation_anomaly'] = df['precipitation'] - df['precipitation'].rolling(
            window=30, min_periods=1
        ).mean()
        
        # Extreme weather events
        df['is_heat_wave'] = (
            (df['temperature'] > df['temperature'].rolling(window=5).mean() + 2 * df['temperature'].rolling(window=5).std()) &
            (df['temperature'] > 30)
        )
        
        df['is_cold_snap'] = (
            (df['temperature'] < df['temperature'].rolling(window=5).mean() - 2 * df['temperature'].rolling(window=5).std()) &
            (df['temperature'] < 0)
        )
        
        return df
    
    def _calculate_climate_indices(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate climate indices"""
        # Standardized Precipitation Index (SPI)
        df['spi'] = self._calculate_spi(df['precipitation'])
        
        # Temperature-Humidity Index (THI)
        df['thi'] = 0.8 * df['temperature'] + df['humidity'] * (df['temperature'] - 14.4) + 46.4
        
        # Comfort Index
        df['comfort_index'] = self._calculate_comfort_index(
            df['temperature'],
            df['humidity'],
            df['wind_speed']
        )
        
        return df
    
    @staticmethod
    def _calculate_heat_index(temperature: pd.Series, humidity: pd.Series) -> pd.Series:
        """Calculate heat index using the Rothfusz regression"""
        return (
            0.5 * (temperature + 61.0 + ((temperature - 68.0) * 1.2) + (humidity * 0.094))
        )
    
    @staticmethod
    def _calculate_wind_chill(temperature: pd.Series, wind_speed: pd.Series) -> pd.Series:
        """Calculate wind chill using the North American formula"""
        return (
            13.12 + 0.6215 * temperature - 11.37 * wind_speed ** 0.16 +
            0.3965 * temperature * wind_speed ** 0.16
        )
    
    @staticmethod
    def _calculate_dew_point(temperature: pd.Series, humidity: pd.Series) -> pd.Series:
        """Calculate dew point using the Magnus formula"""
        a = 17.27
        b = 237.7
        alpha = ((a * temperature) / (b + temperature)) + np.log(humidity/100.0)
        return (b * alpha) / (a - alpha)
    
    @staticmethod
    def _calculate_spi(precipitation: pd.Series) -> pd.Series:
        """Calculate Standardized Precipitation Index"""
        # Fit gamma distribution
        shape, loc, scale = stats.gamma.fit(precipitation.dropna())
        # Calculate SPI
        return stats.norm.ppf(stats.gamma.cdf(precipitation, shape, loc, scale))
    
    @staticmethod
    def _calculate_comfort_index(
        temperature: pd.Series,
        humidity: pd.Series,
        wind_speed: pd.Series
    ) -> pd.Series:
        """Calculate thermal comfort index"""
        return (
            0.5 * temperature +
            0.3 * humidity +
            0.2 * wind_speed
        )

def main():
    # Initialize crawler
    api_key = os.getenv('NOAA_API_TOKEN')
    if not api_key:
        raise ValueError("NOAA_API_TOKEN environment variable not set")
    
    crawler = NOAACrawler(api_key)
    transformer = WeatherTransformer()
    
    try:
        # Example: Fetch data for New York Central Park station
        station_id = "GHCND:USW00094728"
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 12, 31)
        
        # Fetch data
        data = crawler.fetch_historical_data(
            start_date=start_date,
            end_date=end_date,
            station_id=station_id
        )
        
        # Transform data
        df = pd.DataFrame(data)
        transformed_df = transformer.transform_data(df)
        
        # Save results
        output_path = "data/processed/weather_analysis.csv"
        transformed_df.to_csv(output_path, index=False)
        logger.info(f"Data saved to {output_path}")
        
    finally:
        crawler.close()

if __name__ == "__main__":
    main() 