from src.kafka_config import KafkaConfig
from src.extract import get_extractor
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get API key from environment
api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
if not api_key:
    raise ValueError("ALPHA_VANTAGE_API_KEY environment variable not set")

# Initialize Kafka config
kafka_config = KafkaConfig(bootstrap_servers='localhost:9092')

# Get extractor with Kafka integration
extractor = get_extractor(
    source='alphavantage',
    api_key=api_key,
    kafka_config=kafka_config
)

# Extract and publish data
data = extractor.extract()