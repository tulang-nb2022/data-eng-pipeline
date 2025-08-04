import os
import json
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import requests
from confluent_kafka import Producer
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

class WeatherCrawler:
    def __init__(self, config_path: str = "config/weather_config.yml"):
        self.config = self._load_config(config_path)
        self.base_url = "https://api.weather.gov"
        self.producer = self._create_kafka_producer()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.config['api']['user_agent']
        })
        
    def _load_config(self, config_path: str) -> Dict:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _create_kafka_producer(self) -> Producer:
        kafka_config = self.config['kafka']
        config = {
            'bootstrap.servers': kafka_config['bootstrap_servers'],
            'client.id': kafka_config['client_id'],
            'acks': 'all',
            'retries': 3,
            'retry.backoff.ms': 1000,
            'compression.type': 'snappy',
            'linger.ms': 5,
            'batch.size': 16384,
            'queue.buffering.max.messages': 100000,
            'queue.buffering.max.ms': 1000,
            'queue.buffering.max.kbytes': 1024,
            'message.max.bytes': 1000000
        }
        return Producer(config)
    
    def delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')
    
    def get_forecast(self, lat: float, lon: float) -> Dict:
        """Get weather forecast for a specific location"""
        # First, get the grid endpoint
        points_url = f"{self.base_url}/points/{lat},{lon}"
        try:
            response = self.session.get(points_url)
            response.raise_for_status()
            points_data = response.json()
            
            # Get the forecast URL
            forecast_url = points_data['properties']['forecast']
            
            # Get the forecast data
            forecast_response = self.session.get(forecast_url)
            forecast_response.raise_for_status()
            forecast_data = forecast_response.json()
            
            return self._process_forecast_data(forecast_data, lat, lon)
            
        except Exception as e:
            logger.error(f"Error fetching forecast: {str(e)}")
            return {}
    
    def _process_forecast_data(self, forecast_data: Dict, lat: float, lon: float) -> List[Dict]:
        """Process raw forecast data into a list of records"""
        processed_data = []
        
        try:
            periods = forecast_data.get('properties', {}).get('periods', [])
            
            for period in periods:
                record = {
                    'timestamp': datetime.now().isoformat(),
                    'grid_id': f"{lat}_{lon}",
                    'latitude': lat,
                    'longitude': lon,
                    'start_time': period.get('startTime'),
                    'end_time': period.get('endTime'),
                    'temperature': period.get('temperature'),
                    'temperature_unit': period.get('temperatureUnit'),
                    'wind_speed': period.get('windSpeed'),
                    'wind_direction': period.get('windDirection'),
                    'short_forecast': period.get('shortForecast'),
                    'detailed_forecast': period.get('detailedForecast'),
                    'precipitation_probability': period.get('probabilityOfPrecipitation', {}).get('value'),
                    'relative_humidity': period.get('relativeHumidity', {}).get('value'),
                    'dewpoint': period.get('dewpoint', {}).get('value'),
                    'dewpoint_unit': period.get('dewpoint', {}).get('unitCode'),
                    'pressure': period.get('pressure', {}).get('value'),
                    'pressure_unit': period.get('pressure', {}).get('unitCode'),
                    'visibility': period.get('visibility', {}).get('value'),
                    'visibility_unit': period.get('visibility', {}).get('unitCode')
                }
                processed_data.append(record)
                
        except Exception as e:
            logger.error(f"Error processing forecast data: {str(e)}")
            
        return processed_data
    
    def send_to_kafka(self, data: List[Dict]) -> None:
        """Send data to Kafka topic"""
        topic = self.config['kafka']['topic']
        try:
            for record in data:
                self.producer.produce(
                    topic=topic,
                    key=str(record['grid_id']),
                    value=json.dumps(record),
                    callback=self.delivery_report
                )
                # Trigger any available delivery report callbacks
                self.producer.poll(0)
            
            # Wait for all messages to be delivered
            self.producer.flush()
            logger.info(f"Successfully sent {len(data)} records to Kafka")
        except Exception as e:
            logger.error(f"Error sending to Kafka: {str(e)}")
    
    def crawl_weather_data(self, lat: float = 35.4689, lon: float = -97.5195) -> None:
        """Crawl weather data for a specific location"""
        try:
            # Get forecast data
            forecast_data = self.get_forecast(lat, lon)
            
            if forecast_data:
                # Send to Kafka
                self.send_to_kafka(forecast_data)
                
                # Rate limiting
                time.sleep(1)  # NWS API has a rate limit of 1 request per second
            else:
                logger.warning("No forecast data received")
                
        except Exception as e:
            logger.error(f"Error in crawl_weather_data: {str(e)}")
    
    def close(self) -> None:
        """Close the Kafka producer"""
        self.producer.flush()

def main():
    # Initialize crawler
    crawler = WeatherCrawler()
    
    try:
        # Crawl weather data for the specified location
        crawler.crawl_weather_data()
        
    finally:
        crawler.close()

if __name__ == "__main__":
    main() 