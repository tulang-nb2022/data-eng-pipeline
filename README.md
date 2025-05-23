# Financial Data Crawler

A robust data pipeline for collecting and processing financial market data using Alpha Vantage API and Kafka.

## Features

- Real-time market data collection during trading hours
- Rate-limited API requests to comply with Alpha Vantage limits
- Kafka integration for data streaming
- Technical indicators calculation (RSI, MACD)
- Market sentiment analysis
- Data transformation and storage in PostgreSQL
- Comprehensive error handling and logging

## Prerequisites

- Python 3.8+
- PostgreSQL 12+
- Apache Kafka
- Alpha Vantage API key

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd cursor-data-eng
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
```
Edit `.env` with your configuration:
- Add your Alpha Vantage API key
- Configure database credentials
- Set Kafka bootstrap servers if different from default

5. Set up the database:
```bash
# Create database and schema
createdb financial_analytics
psql financial_analytics -c "CREATE SCHEMA IF NOT EXISTS public;"
```

6. Initialize dbt:
```bash
dbt deps
dbt run
```

## Usage

### Starting the Crawler

1. Ensure Kafka is running:
```bash
# Start Zookeeper (if not running as a service)
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka server
bin/kafka-server-start.sh config/server.properties
```

2. Start the crawler:
```bash
python examples/run_crawler.py
```

The crawler will:
- Run during market hours (9:30 AM - 4:00 PM ET)
- Make 25 evenly-spaced requests per day
- Publish data to Kafka topics
- Log activities to `crawler.log`

### Stopping the Crawler

The crawler can be stopped using:
- `Ctrl+C` in the terminal
- The process will gracefully shut down and flush any pending Kafka messages

### Monitoring

1. Check crawler logs:
```bash
tail -f crawler.log
```

2. Monitor Kafka topics:
```bash
# List all topics
kafka-topics.sh --list --bootstrap-server localhost:9092

# View messages in a topic
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic market.intraday --from-beginning
```

3. Check database tables:
```sql
-- Connect to database
psql financial_analytics

-- View recent data
SELECT * FROM fct_stock_performance 
ORDER BY trading_date DESC 
LIMIT 5;

-- Check technical indicators
SELECT * FROM int_technical_indicators 
WHERE trading_date = CURRENT_DATE;
```

## Data Flow

1. **Data Collection**
   - Alpha Vantage API → Raw JSON files in `data/raw/`
   - Kafka topics: `market.raw`, `market.intraday`, `market.sentiment`, `market.indicators`

2. **Data Processing**
   - Raw data → Staging tables
   - Technical indicators calculation
   - Performance metrics computation

3. **Data Storage**
   - Processed data → PostgreSQL
   - Tables: `fct_stock_performance`, `int_technical_indicators`

## Project Structure

```
.
├── data/
│   └── raw/           # Raw JSON data files
├── models/
│   ├── staging/       # Raw data models
│   ├── intermediate/  # Technical indicators
│   └── marts/         # Business-ready models
├── src/
│   ├── extract.py     # Data extraction
│   ├── kafka_config.py # Kafka configuration
│   └── utils.py       # Utility functions
├── examples/
│   └── run_crawler.py # Crawler execution script
├── .env.example       # Environment variables template
└── requirements.txt   # Python dependencies
```

## Error Handling

- All errors are logged to `crawler.log`
- Failed API requests are retried with exponential backoff
- Kafka message delivery failures are logged
- Database connection issues trigger appropriate error handling

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Your License Here] 