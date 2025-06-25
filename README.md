# Real-Time Data Pipeline

A robust real-time data pipeline for collecting and processing financial market data and weather data using Kafka streaming, Spark, and AWS services. This project focuses on real-time data collection, processing, and analysis with streaming capabilities.

## Features

- Real-time data collection from Alpha Vantage API (financial) and NOAA (weather)
- Rate-limited API requests to comply with API limits
- Kafka integration for real-time data streaming with multiple topics
- Spark Streaming for real-time data transformation
- Comprehensive data transformation and enrichment
- Error handling and logging with centralized error management
- Background crawler service with graceful shutdown
- Pre-commit hooks for security scanning
- Data validation with Great Expectations
- Apache Airflow for workflow orchestration
- AWS S3 storage with Parquet format
- Amazon Athena for interactive querying

## Prerequisites

- Python 3.8+
- Apache Kafka
- Apache Spark 3.2.0
- Scala 2.12.15
- Alpha Vantage API key
- AWS Account with:
  - S3 bucket
  - Athena database
  - IAM roles and permissions
- Apache Airflow
- Great Expectations

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
- Configure AWS credentials
- Set Kafka bootstrap servers

5. Set up Kafka:
```bash
# Using Docker (recommended)
docker run -d --name kafka -p 9092:9092 apache/kafka:2.13-3.2.0

# Create topics
docker exec kafka kafka-topics.sh --create --topic weather-forecast --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec kafka kafka-topics.sh --create --topic financial-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

6. Set up AWS resources:
```bash
# Create S3 bucket
aws s3 mb s3://your-data-bucket

# Create Athena database
aws athena start-query-execution \
    --query-string "CREATE DATABASE IF NOT EXISTS data_analytics" \
    --result-configuration "OutputLocation=s3://your-bucket/athena-results/"
```

7. Initialize Great Expectations:
```bash
great_expectations init
```

8. Set up Airflow:
```bash
# Initialize Airflow
airflow db init

# Create Airflow user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email your_email \
    --password your_password
```

## Usage

### Starting the Crawler

1. Ensure Kafka is running:
```bash
# Check if Kafka is running
nc -z localhost 9092
```

2. Start the crawler service:
```bash
python src/crawler_service.py
```

The crawler service will:
- Run as a background process
- Collect financial data during market hours (9:30 AM - 4:00 PM ET)
- Respect Alpha Vantage rate limits (25 requests/day)
- Publish data to Kafka topics: `financial-data`, `weather-forecast`
- Log activities to `crawler.log`
- Handle graceful shutdown on system signals

### Running Real-Time Data Transformations

1. Make the script executable:
```bash
chmod +x run_transform.sh
```

2. Run the streaming transformation job:
```bash
# For weather data
./run_transform.sh weather-forecast noaa data/processed

# For financial data
./run_transform.sh financial-data alphavantage data/processed

# For EOSDIS data
./run_transform.sh eosdis-data eosdis data/processed
```

The streaming transformation job will:
- Build the Scala project using SBT
- Read real-time data from Kafka topics
- Apply transformations using Spark Streaming
- Write processed data to local storage in Parquet format
- Handle different data types with automatic detection

### Testing the Kafka Streaming Setup

1. Test the complete pipeline:
```bash
chmod +x scripts/test_kafka_streaming.sh
./scripts/test_kafka_streaming.sh
```

2. This will:
- Check if Kafka is running
- Create the weather-forecast topic
- Send test weather data messages
- Provide instructions to run the transformation

### Data Collection

The crawler collects the following data types:
- **Financial Data**: Real-time price data (open, high, low, close, volume)
- **Weather Data**: Temperature, wind speed, precipitation probability
- **Sentiment Data**: News sentiment analysis and scores
- **Technical Indicators**: RSI, MACD, and other technical analysis metrics

### Data Validation

1. Run Great Expectations validation:
```bash
python great_expectations/weather_data_suite.py
```

2. View validation results:
```bash
# Check S3 for validation results
aws s3 ls s3://your-validation-results-bucket/
```

### Monitoring with Airflow

1. Start Airflow services:
```bash
airflow webserver -p 8080
airflow scheduler
```

2. Access the Airflow UI at http://localhost:8080

3. Monitor data quality:
- Check the `weather_data_monitoring` DAG
- View task logs and execution history
- Monitor data quality scores
- Track anomalies and issues

### Querying Data with Athena

1. Access Athena console in AWS
2. Select the `data_analytics` database
3. Run example queries:
```sql
-- Check data quality scores
SELECT 
    symbol,
    AVG(data_quality_score) as avg_quality_score,
    COUNT(*) as record_count
FROM market_data
WHERE date >= current_date - interval '1' day
GROUP BY symbol;

-- Find price anomalies
SELECT 
    symbol,
    date,
    close_price,
    price_change_pct
FROM market_data
WHERE abs(price_change_pct) > 5
    AND date >= current_date - interval '1' day;
```

## Data Flow

1. **Data Collection**
   - Alpha Vantage API → Raw JSON data
   - Kafka topics: `market.intraday`, `market.sentiment`, `market.indicators`, `market.raw`

2. **Data Processing**
   - Raw data → Data transformation and enrichment
   - Technical indicators calculation
   - Data quality scoring and validation

3. **Data Storage**
   - Processed data → S3 (Parquet format)
   - Partitioned by year/month/day
   - Optimized for Athena queries

4. **Data Validation**
   - Great Expectations validation suite
   - Automated quality checks
   - Validation results stored in S3

5. **Monitoring**
   - Airflow DAGs for workflow orchestration
   - Data quality monitoring
   - Anomaly detection
   - Performance metrics

## Project Structure

```
.
├── data/
│   └── raw/           # Raw JSON data files
├── models/
│   ├── staging/       # Raw data models
│   ├── intermediate/  # Financial metrics
│   └── marts/         # Business-ready models
├── src/
│   ├── crawler/       # Crawler implementation
│   │   └── noaa_crawler.py
│   ├── main/
│   │   └── python/    # Python utilities
│   ├── extract.py     # Data extraction
│   ├── transform.py   # Data transformation
│   ├── kafka_config.py # Kafka configuration
│   ├── crawler_service.py # Background crawler service
│   └── utils.py       # Utility functions
├── great_expectations/
│   └── weather_data_suite.py
├── dags/
│   └── weather_data_monitoring.py
├── config/
│   └── weather_config.yml
├── requirements.txt
└── build.sbt          # Scala build configuration
```

## Configuration

### Alpha Vantage API
- Free tier: 25 requests per day
- Rate limiting implemented to respect limits
- Trading hours: 9:30 AM - 4:00 PM ET (Monday-Friday)

### Kafka Topics
- `market.intraday`: Real-time price data
- `market.sentiment`: News sentiment analysis
- `market.indicators`: Technical indicators
- `market.raw`: Raw API responses

### S3 Storage
- Raw data: `s3://your-bucket/raw/`
- Processed data: `s3://your-bucket/processed/`
- Checkpoints: `s3://your-bucket/checkpoints/`

## Security

The project includes a pre-commit hook that prevents accidental commits of sensitive information:
- API keys and tokens
- Passwords and credentials
- Private keys
- Email addresses
- IP addresses
- Credit card numbers
- Social security numbers

To ensure the hook is active:
```bash
# Make the hook executable
chmod +x .git/hooks/pre-commit

# Test the hook
git add .
git commit -m "Test commit"
```

## Error Handling

The pipeline includes comprehensive error handling:
- API rate limit management
- Network timeout handling
- Data validation errors
- Kafka connection issues
- Graceful shutdown on system signals

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Your License Here] 

# Data Engineering Project

A comprehensive financial data pipeline using Alpha Vantage API, Kafka, Spark, Delta Lake, AWS Athena, Great Expectations, and Airflow.

## Architecture

- **Data Extraction**: Alpha Vantage API with rate limiting and trading schedule awareness
- **Streaming**: Kafka for real-time data ingestion
- **Processing**: Spark with Delta Lake for data transformation
- **Storage**: AWS S3 for data lake storage
- **Query**: AWS Athena for interactive queries
- **Validation**: Great Expectations for data quality
- **Orchestration**: Apache Airflow for workflow management

## Prerequisites

- Python 3.8+
- Scala 2.12.15
- Apache Spark 3.2.0
- Apache Kafka 2.8.0
- Apache Airflow 2.5.0
- AWS CLI configured with appropriate permissions

## Setup

### 1. Environment Setup

```bash
# Install Python dependencies
pip install -r requirements.txt

# Install Scala dependencies
sbt compile
```

### 2. AWS Configuration

The project uses AWS CLI profile for authentication. No AWS keys are stored in the code.

**Setup AWS CLI:**
```bash
# Configure AWS CLI with your credentials
aws configure

# Or use a specific profile
aws configure --profile data-eng
```

**Required AWS Permissions:**
- S3: Read/Write access to your data buckets
- Athena: Query execution permissions
- IAM: Role assumption (if using EC2/ECS)

### 3. Kafka Setup

```bash
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka
bin/kafka-server-start.sh config/server.properties
```

### 4. Airflow Setup

```bash
# Initialize Airflow database
airflow db init

# Create Airflow user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email your_email \
    --password your_password
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

2. Start the crawler service:
```bash
python src/crawler_service.py
```

The crawler service will:
- Run as a background process
- Collect financial data during market hours (9:30 AM - 4:00 PM ET)
- Respect Alpha Vantage rate limits (25 requests/day)
- Publish data to Kafka topics: `market.intraday`, `market.sentiment`, `market.indicators`, `market.raw`
- Log activities to `crawler.log`
- Handle graceful shutdown on system signals

### Running Data Transformations

1. Configure the transformation job:
```bash
# Edit run_transform.sh with your specific paths
nano run_transform.sh
```

2. Make the script executable:
```bash
chmod +x run_transform.sh
```

3. Run the transformation job:
```bash
./run_transform.sh
```

The transformation job will:
- Build the Scala project using SBT
- Read raw data from S3
- Apply transformations using Spark
- Write processed data back to S3 in Parquet format
- Handle different data types (intraday, sentiment, indicators)

**AWS Authentication:**
The `DataTransformerApp` automatically uses your AWS CLI profile credentials through the `DefaultAWSCredentialsProviderChain`. This means:
- No AWS keys are stored in code or configuration files
- Credentials are securely managed by AWS CLI
- Supports IAM roles, temporary credentials, and profile switching
- Works seamlessly with AWS SSO and other authentication methods

### Data Collection

The crawler collects the following data types:
- **Intraday Data**: Real-time price data (open, high, low, close, volume)
- **Sentiment Data**: News sentiment analysis and scores
- **Technical Indicators**: RSI, MACD, and other technical analysis metrics

### Data Validation

1. Run Great Expectations validation:
```bash
python great_expectations/weather_data_suite.py
```

2. View validation results:
```bash
# Check S3 for validation results
aws s3 ls s3://your-validation-results-bucket/
```

### Monitoring with Airflow

1. Start Airflow services:
```bash
airflow webserver -p 8080
airflow scheduler
```

2. Access Airflow UI:
```
http://localhost:8080
```

## Security Features

### Pre-commit Hook
A pre-commit hook prevents committing sensitive information:
- API keys
- Passwords
- Private keys
- Email addresses
- IP addresses
- Credit card numbers
- Social security numbers

### Environment Variables
Sensitive configuration is managed through environment variables:
```bash
export ALPHA_VANTAGE_API_KEY=your_api_key
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

## Troubleshooting

### Common Issues

1. **S3 Access Denied**: Ensure your AWS CLI profile has S3 permissions
2. **Kafka Connection Failed**: Check if Kafka is running on the correct port
3. **Rate Limit Exceeded**: Alpha Vantage free tier has 25 requests/day limit
4. **Build Failures**: Ensure all dependencies are installed and versions match

### Debug Mode
Enable debug logging:
```bash
export LOG_LEVEL=DEBUG
python src/crawler_service.py
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and validation
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 