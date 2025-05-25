import os
from dotenv import load_dotenv
from src.crawler_service import CrawlerService
from src.kafka_config import KafkaConfig
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    # Load environment variables from .env file
    load_dotenv()
    
    # Get API key from environment variable
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        raise ValueError("ALPHA_VANTAGE_API_KEY environment variable not set")
    
    # Get Kafka configuration
    kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    kafka_config = KafkaConfig(bootstrap_servers=kafka_bootstrap_servers)
    
    try:
        # Initialize and start crawler service
        crawler = CrawlerService(api_key=api_key, kafka_config=kafka_config)
        crawler.start()
        
        # Keep the main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt. Shutting down...")
        crawler.stop()
    except Exception as e:
        logger.error(f"Error during crawler execution: {str(e)}")
        crawler.stop()
        raise

if __name__ == "__main__":
    main() 