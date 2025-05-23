from typing import Dict, Any
from confluent_kafka import Producer
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaConfig:
    # Kafka topics
    TOPICS = {
        'intraday': 'market.intraday',
        'sentiment': 'market.sentiment',
        'indicators': 'market.indicators',
        'raw_data': 'market.raw'
    }
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        """Initialize Kafka configuration"""
        self.bootstrap_servers = bootstrap_servers
        self.producer = self._create_producer()
    
    def _create_producer(self) -> Producer:
        """Create and return a Kafka producer"""
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': 'market_data_producer',
            'acks': 'all',  # Wait for all replicas to acknowledge
            'retries': 3,   # Number of retries on failure
            'retry.backoff.ms': 1000,  # Time to wait between retries
            'compression.type': 'snappy'  # Compress messages
        }
        return Producer(config)
    
    def delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')
    
    def produce(self, topic: str, key: str, value: Dict[str, Any]):
        """Produce a message to Kafka"""
        try:
            # Convert value to JSON string
            value_str = json.dumps(value)
            
            # Produce message
            self.producer.produce(
                topic=topic,
                key=key,
                value=value_str,
                callback=self.delivery_report
            )
            
            # Trigger any available delivery report callbacks
            self.producer.poll(0)
            
        except Exception as e:
            logger.error(f'Error producing message to {topic}: {str(e)}')
            raise
    
    def flush(self):
        """Wait for all messages to be delivered"""
        self.producer.flush() 