from typing import Dict, Any
from confluent_kafka import Producer, Consumer, KafkaError
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
        self.consumer = self._create_consumer()
    
    def _create_producer(self) -> Producer:
        """Create and return a Kafka producer"""
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': 'market_data_producer',
            'acks': 'all',  # Wait for all replicas to acknowledge
            'retries': 3,   # Number of retries on failure
            'retry.backoff.ms': 1000,  # Time to wait between retries
            'compression.type': 'snappy',  # Compress messages
            'linger.ms': 5,  # Wait for up to 5ms to batch messages
            'batch.size': 16384,  # 16KB batch size
            'queue.buffering.max.messages': 100000,  # Maximum number of messages in queue
            'queue.buffering.max.ms': 1000,  # Maximum time to buffer messages
            'queue.buffering.max.kbytes': 1024,  # Maximum queue size in KB
            'message.max.bytes': 1000000  # Maximum message size
        }
        return Producer(config)
    
    def _create_consumer(self) -> Consumer:
        """Create and return a Kafka consumer"""
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': 'market_data_consumer',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000,
            'max.poll.interval.ms': 300000,
            'session.timeout.ms': 10000,
            'heartbeat.interval.ms': 3000,
            'max.partition.fetch.bytes': 1048576,  # 1MB
            'fetch.message.max.bytes': 1048576,  # 1MB
            'queued.max.messages.kbytes': 1024,  # 1MB
            'queued.min.messages': 100
        }
        return Consumer(config)
    
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
    
    def consume(self, topics: list, timeout: float = 1.0) -> list:
        """Consume messages from Kafka topics"""
        try:
            self.consumer.subscribe(topics)
            messages = []
            
            while True:
                msg = self.consumer.poll(timeout)
                if msg is None:
                    break
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f'Error consuming message: {msg.error()}')
                        break
                
                try:
                    value = json.loads(msg.value().decode('utf-8'))
                    messages.append({
                        'topic': msg.topic(),
                        'partition': msg.partition(),
                        'offset': msg.offset(),
                        'key': msg.key().decode('utf-8') if msg.key() else None,
                        'value': value
                    })
                except json.JSONDecodeError as e:
                    logger.error(f'Error decoding message: {str(e)}')
                    continue
            
            return messages
            
        except Exception as e:
            logger.error(f'Error consuming messages: {str(e)}')
            raise
        finally:
            self.consumer.unsubscribe()
    
    def flush(self):
        """Wait for all messages to be delivered"""
        self.producer.flush()
    
    def close(self):
        """Close producer and consumer"""
        self.producer.flush()
        self.producer.close()
        self.consumer.close() 