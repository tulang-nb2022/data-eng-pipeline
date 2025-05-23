import os
from dotenv import load_dotenv
from src.extract import get_extractor
from src.kafka_config import KafkaConfig
import logging

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
        # Initialize extractor
        extractor = get_extractor(
            source='alphavantage',
            api_key=api_key,
            kafka_config=kafka_config
        )
        
        # Run extraction
        logger.info("Starting data extraction...")
        data = extractor.extract()
        logger.info("Data extraction completed successfully")
        
    except Exception as e:
        logger.error(f"Error during data extraction: {str(e)}")
        raise

if __name__ == "__main__":
    main() 