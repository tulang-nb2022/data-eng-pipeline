import os
import time
import signal
import threading
from datetime import datetime, timedelta
import logging
from typing import Optional
from .extract import get_extractor
from .utils import TradingSchedule
from .kafka_config import KafkaConfig

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

class CrawlerService:
    def __init__(self, api_key: str, kafka_config: Optional[KafkaConfig] = None):
        self.api_key = api_key
        self.kafka_config = kafka_config or KafkaConfig()
        self.trading_schedule = TradingSchedule()
        self.running = False
        self._stop_event = threading.Event()
        self._crawler_thread: Optional[threading.Thread] = None
        
        # Register signal handlers
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
    
    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info("Received shutdown signal. Stopping crawler...")
        self.stop()
    
    def _sleep_until_next_request(self) -> bool:
        """Sleep until next request time or return False if should stop"""
        next_time = self.trading_schedule.get_next_request_time()
        if not next_time:
            return False
            
        now = datetime.now(self.trading_schedule.ny_tz)
        sleep_seconds = (next_time - now).total_seconds()
        
        if sleep_seconds <= 0:
            return True
            
        # Sleep in small intervals to allow for graceful shutdown
        while sleep_seconds > 0 and not self._stop_event.is_set():
            sleep_interval = min(sleep_seconds, 1.0)  # Check every second
            time.sleep(sleep_interval)
            sleep_seconds -= sleep_interval
            
        return not self._stop_event.is_set()
    
    def _crawler_loop(self):
        """Main crawler loop"""
        extractor = get_extractor(
            source='alphavantage',
            api_key=self.api_key,
            kafka_config=self.kafka_config
        )
        
        while not self._stop_event.is_set():
            try:
                if not self.trading_schedule.is_market_open():
                    logger.info("Market is closed. Waiting for next market open...")
                    if not self._sleep_until_next_request():
                        break
                    continue
                
                logger.info("Starting data collection cycle...")
                data = extractor.extract()
                logger.info("Data collection cycle completed")
                
                # Sleep until next request time
                if not self._sleep_until_next_request():
                    break
                    
            except Exception as e:
                logger.error(f"Error in crawler loop: {str(e)}")
                # Sleep for a short time before retrying
                time.sleep(5)
    
    def start(self):
        """Start the crawler service"""
        if self.running:
            logger.warning("Crawler is already running")
            return
            
        self.running = True
        self._stop_event.clear()
        
        logger.info("Starting crawler service...")
        self._crawler_thread = threading.Thread(target=self._crawler_loop)
        self._crawler_thread.daemon = True
        self._crawler_thread.start()
    
    def stop(self):
        """Stop the crawler service gracefully"""
        if not self.running:
            return
            
        logger.info("Stopping crawler service...")
        self._stop_event.set()
        
        if self._crawler_thread:
            self._crawler_thread.join(timeout=30)
            
        self.running = False
        logger.info("Crawler service stopped") 