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

class WeatherCrawler:
    def __init__(self, config_path: str = "config/weather_config.yml"):
        self.config = self._load_config(config_path)
        self.base_url = "https://api.weather.gov"
        self.producer = self._create_kafka_producer()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': '(Weather Data Crawler, your_email)'
        })
        
    def _load_config(self, config_path: str) -> Dict:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _create_kafka_producer(self) -> KafkaProducer:
        kafka_config = self.config['kafka']
        return KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            api_version=(0, 10, 2),
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
    
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
    
    def _process_forecast_data(self, forecast_data: Dict, lat: float, lon: float) -> Dict:
        """Process raw forecast data into standardized format"""
        periods = forecast_data['properties']['periods']
        processed_data = []
        
        for period in periods:
            processed_period = {
                'timestamp': period['startTime'],
                'temperature_celsius': self._fahrenheit_to_celsius(period['temperature']),
                'wind_speed': self._convert_wind_speed(period['windSpeed']),
                'wind_direction': period['windDirection'],
                'forecast': period['shortForecast'],
                'detailed_forecast': period['detailedForecast'],
                'humidity': period.get('relativeHumidity', {}).get('value'),
                'precipitation_probability': period.get('probabilityOfPrecipitation', {}).get('value'),
                'latitude': lat,
                'longitude': lon,
                'processing_timestamp': datetime.utcnow().isoformat()
            }
            processed_data.append(processed_period)
        
        return processed_data
    
    def _fahrenheit_to_celsius(self, fahrenheit: float) -> float:
        """Convert Fahrenheit to Celsius"""
        return (fahrenheit - 32) * 5/9
    
    def _convert_wind_speed(self, wind_speed_str: str) -> float:
        """Convert wind speed string to m/s"""
        try:
            # Extract numeric value and unit
            parts = wind_speed_str.split()
            value = float(parts[0])
            unit = parts[1].lower()
            
            # Convert to m/s
            if unit == 'mph':
                return value * 0.44704
            elif unit == 'kph':
                return value * 0.277778
            else:
                return value
        except:
            return 0.0
    
    def send_to_kafka(self, data: List[Dict]) -> None:
        """Send data to Kafka topic"""
        topic = self.config['kafka']['topic']
        try:
            for record in data:
                self.producer.send(topic, value=record)
            self.producer.flush()
            logger.info(f"Successfully sent {len(data)} records to Kafka")
        except Exception as e:
            logger.error(f"Error sending to Kafka: {str(e)}")
    
    def crawl_weather_data(self, lat: float = 55.3422, lon: float = -131.6461) -> None:
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
        self.producer.close()

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