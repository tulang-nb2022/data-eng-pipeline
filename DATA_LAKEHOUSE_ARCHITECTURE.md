# Data Lakehouse Architecture

## Overview

This project implements a modern data lakehouse architecture with Bronze, Silver, and Gold layers using Apache Spark, Delta Lake, and dbt.

## Architecture Layers

### 🥉 Bronze Layer (Raw Data Ingestion)
- **Purpose**: Raw data ingestion from Kafka with minimal processing
- **Technology**: Spark Streaming + Delta Lake
- **Location**: S3 Bronze bucket
- **Schema**: Preserves original data structure with Kafka metadata

**Columns**:
- `temperature`, `wind_speed`, `city`, `timestamp`, `humidity`, `pressure`, `visibility`
- `processing_timestamp`, `hour`, `minute`
- `kafka_offset`, `kafka_partition`, `kafka_timestamp`
- `data_source` (noaa)

### 🥈 Silver Layer (Cleaned & Enriched Data)
- **Purpose**: Data cleaning, deduplication, schema enforcement, and enrichment
- **Technology**: Spark Batch + Delta Lake
- **Location**: S3 Silver bucket
- **Schema**: Cleaned data with quality scores and validation flags

**Additional Columns**:
- `year`, `month`, `day` (partitioning columns)
- `quality_score` (0.0-1.0)
- `is_valid` (boolean validation flag)

### 🥇 Gold Layer (Business Metrics)
- **Purpose**: Aggregated business metrics and KPIs
- **Technology**: dbt + Delta Lake
- **Location**: S3 Gold bucket
- **Schema**: Daily/hourly aggregated metrics with weather alerts

**Metrics**:
- Temperature: avg, max, min, stddev
- Humidity: avg, max, min
- Pressure: avg, max, min
- Wind: avg, max
- Visibility: avg, min
- Quality: avg quality score
- Alerts: weather alert types and severity

## Data Flow

```
Kafka → Bronze (Spark Streaming) → Silver (Spark Batch) → Gold (dbt)
```

### 1. Data Ingestion (Bronze)
```bash
# Start bronze layer streaming job
./run_transform.sh bronze weather-forecast noaa s3://my-bucket/bronze/weather

# In another terminal, send data to Kafka
python src/crawler/noaa_crawler.py
```

### 2. Data Processing (Silver)
```bash
# Run silver layer batch job
./run_transform.sh silver weather-forecast noaa s3://my-bucket/bronze/weather s3://my-bucket/silver/weather
```

### 3. Data Analytics (Gold)
```bash
# Run dbt models for gold layer
dbt run --models gold
```

## Usage Examples

### Individual Layers
```bash
# Bronze layer only (streaming)
./run_transform.sh bronze weather-forecast noaa s3://my-bucket/bronze/weather

# Silver layer only (batch)
./run_transform.sh silver weather-forecast noaa s3://my-bucket/bronze/weather s3://my-bucket/silver/weather

# Gold layer only (batch)
./run_transform.sh gold weather-forecast noaa s3://my-bucket/bronze/weather s3://my-bucket/silver/weather s3://my-bucket/gold/weather
```

### Complete Pipeline
```bash
# Run all layers sequentially
./run_transform.sh all weather-forecast noaa s3://my-bucket/bronze/weather s3://my-bucket/silver/weather s3://my-bucket/gold/weather
```

## Data Quality & Validation

### Bronze Layer Validation
- Minimal validation (preserves raw data)
- Kafka metadata tracking
- Basic null handling

### Silver Layer Validation
- **Deduplication**: Based on key fields
- **Data Cleaning**: Type conversions, regex cleaning
- **Quality Scoring**: 0.0-1.0 based on completeness
- **Validation Rules**:
  - Temperature: -90°C to 60°C
  - Humidity: 0-100%
  - Pressure: 800-1100 hPa
  - Wind Speed: ≥ 0
  - Visibility: ≥ 0

### Gold Layer Validation
- **Business Rules**: Weather alert thresholds
- **Aggregation Logic**: Statistical calculations
- **Data Freshness**: Processing timestamps

## Great Expectations Integration

The validation framework works with all layers:

```bash
# Validate bronze data
python great_expectations/simple_validation.py s3 s3://my-bucket/bronze/weather

# Validate silver data  
python great_expectations/simple_validation.py s3 s3://my-bucket/silver/weather

# Validate gold data
python great_expectations/simple_validation.py s3 s3://my-bucket/gold/weather
```

## S3 Bucket Structure

```
s3://my-data-bucket/
├── bronze/
│   └── weather/
│       ├── year=2024/month=01/day=15/
│       └── year=2024/month=01/day=16/
├── silver/
│   └── weather/
│       ├── year=2024/month=01/day=15/
│       └── year=2024/month=01/day=16/
└── gold/
    └── weather/
        ├── year=2024/month=01/day=15/
        └── year=2024/month=01/day=16/
```

## Delta Lake Benefits

- **ACID Transactions**: Ensures data consistency
- **Schema Evolution**: Handles schema changes gracefully
- **Time Travel**: Query historical versions of data
- **Upserts**: Efficient updates and merges
- **Compaction**: Automatic file optimization

## Monitoring & Observability

### Data Quality Metrics
- Record counts per layer
- Quality scores distribution
- Validation failure rates
- Processing latency

### Business Metrics
- Weather alert frequencies
- Data source distribution
- Geographic coverage
- Temporal patterns

## Configuration

### Environment Variables
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
export SPARK_HOME=/path/to/spark
```

### S3 Bucket Configuration
Update the S3 paths in `run_transform.sh`:
```bash
BRONZE_S3_PATH="s3://your-bucket/bronze/weather"
SILVER_S3_PATH="s3://your-bucket/silver/weather"  
GOLD_S3_PATH="s3://your-bucket/gold/weather"
```

## Troubleshooting

### Common Issues

1. **Kafka Connection**: Ensure Kafka is running on localhost:9092
2. **S3 Access**: Verify AWS credentials and bucket permissions
3. **Delta Lake**: Ensure Delta Lake packages are included
4. **Schema Mismatch**: Check data types between layers

### Debug Commands
```bash
# Check Kafka topics
kafka-topics.sh --list --bootstrap-server localhost:9092

# Monitor Kafka messages
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic weather-forecast --from-beginning

# Check S3 data
aws s3 ls s3://your-bucket/bronze/weather/ --recursive

# Validate data
python great_expectations/simple_validation.py s3 s3://your-bucket/bronze/weather
```

## Performance Optimization

### Spark Configuration
- Adaptive Query Execution (AQE) enabled
- Dynamic partition pruning
- Column pruning and predicate pushdown
- Delta Lake Z-ordering for better performance

### Partitioning Strategy
- Daily partitioning by `year/month/day`
- Optimal file sizes (128MB-1GB)
- Regular compaction and vacuum operations

## Security

### Data Protection
- Encryption at rest (S3)
- Encryption in transit (TLS)
- Access control via IAM policies
- Audit logging for all operations

### Compliance
- Data lineage tracking
- GDPR compliance features
- Data retention policies
- Privacy-preserving analytics