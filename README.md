# Weather Data Crawler

A robust data pipeline for collecting and processing weather data using NOAA API, Kafka, and AWS services. This project has evolved from a financial data crawler to focus on weather data analysis and climate monitoring.

> **Note**: The financial data crawler functionality is now deprecated. This version focuses on weather data collection and analysis.

## Features

- Historical and real-time weather data collection
- Rate-limited API requests to comply with NOAA API limits
- Kafka integration for data streaming
- AWS S3 storage with Parquet format
- Amazon Athena for interactive querying
- Great Expectations for data validation
- Apache Airflow for workflow orchestration
- Advanced weather metrics calculation:
  - Heat Index and Wind Chill
  - Dew Point and Precipitation Intensity
  - Standardized Precipitation Index (SPI)
  - Temperature-Humidity Index (THI)
- Weather pattern detection:
  - Heat waves and cold snaps
  - Temperature and precipitation anomalies
  - Climate indices
- Real-time data quality monitoring
- Comprehensive error handling and logging
- Efficient background service with configurable collection intervals

## Prerequisites

- Python 3.8+
- PostgreSQL 12+
- Apache Kafka
- NOAA API token
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
- Add your NOAA API token
- Configure AWS credentials
- Set Kafka bootstrap servers
- Configure database credentials

5. Set up AWS resources:
```bash
# Create S3 bucket
aws s3 mb s3://your-weather-data-bucket

# Create Athena database
aws athena start-query-execution \
    --query-string "CREATE DATABASE IF NOT EXISTS weather_analytics" \
    --result-configuration "OutputLocation=s3://your-bucket/athena-results/"
```

6. Initialize Great Expectations:
```bash
great_expectations init
```

7. Set up Airflow:
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
# Start Zookeeper (if not running as a service)
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka server
bin/kafka-server-start.sh config/server.properties
```

2. Start the crawler service:
```bash
python src/crawler/noaa_crawler.py
```

The crawler service will:
- Run as a background process
- Collect weather data based on configured intervals
- Publish data to Kafka topics
- Write data to S3 in Parquet format
- Log activities to `crawler.log`
- Handle graceful shutdown on system signals

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
2. Select the `weather_analytics` database
3. Run example queries:
```sql
-- Check data quality scores
SELECT 
    station_id,
    AVG(data_quality_score) as avg_quality_score,
    COUNT(*) as record_count
FROM weather_data
WHERE date >= current_date - interval '1' day
GROUP BY station_id;

-- Find temperature anomalies
SELECT 
    station_id,
    date,
    temperature_celsius,
    temperature_anomaly
FROM weather_data
WHERE temperature_anomaly = true
    AND date >= current_date - interval '1' day;
```

## Data Flow

1. **Data Collection**
   - NOAA API → Raw JSON files in `data/raw/`
   - Kafka topics: `weather.raw`, `weather.metrics`, `weather.patterns`, `weather.indices`

2. **Data Processing**
   - Raw data → Spark Structured Streaming
   - Weather metrics calculation
   - Pattern detection and climate indices computation
   - Data quality scoring

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
│   ├── intermediate/  # Weather metrics
│   └── marts/         # Business-ready models
├── src/
│   ├── crawler/       # Crawler implementation
│   │   └── noaa_crawler.py
│   ├── transform/     # Data transformation
│   └── utils/         # Utility functions
├── great_expectations/
│   └── weather_data_suite.py
├── dags/
│   └── weather_data_monitoring.py
├── config/
│   └── noaa_config.yml
├── .env.example
└── requirements.txt
```

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

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Your License Here] 