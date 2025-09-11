# Silver Layer Batch Job

## Overview

The Silver Layer Batch Job is a standalone Spark application that processes bronze layer data and creates cleaned, enriched silver layer data with comprehensive data quality rules and business logic.

## Architecture

### Input: Bronze Layer Data
- **Format**: Delta Lake tables in S3
- **Schema**: Raw data with Kafka metadata
- **Partitioning**: By year/month/day

### Output: Silver Layer Data
- **Format**: Delta Lake tables in S3
- **Schema**: Cleaned and enriched data
- **Partitioning**: By year/month/day

## Features

### 🔧 Data Quality Processing
- **Data Type Conversions**: Automatic type casting and validation
- **Data Cleaning**: Regex cleaning, null handling, outlier detection
- **Deduplication**: Based on key fields (keeps latest record)
- **Quality Scoring**: 0.0-1.0 based on data completeness and validity

### 📊 Business Logic
- **Weather Categories**: HOT, COLD, WINDY, FOGGY, NORMAL
- **Data Freshness**: Hours between original timestamp and processing
- **Validation Rules**: Comprehensive range checks for all weather metrics

### ⚡ Performance Optimizations
- **Adaptive Query Execution**: Automatic optimization
- **Delta Lake Optimizations**: Auto-compaction and optimization
- **Partitioning**: Efficient data organization
- **Caching**: Intelligent data caching

## Usage

### Basic Usage
```bash
# Run with default parameters
./run_silver_batch.sh

# Run with custom S3 paths
./run_silver_batch.sh s3://my-bucket/bronze/weather s3://my-bucket/silver/weather

# Run for specific date
./run_silver_batch.sh s3://my-bucket/bronze/weather s3://my-bucket/silver/weather 2024-01-15
```

### Direct Spark Submit
```bash
spark-submit \
  --class batch.SilverLayerBatchJob \
  --packages io.delta:delta-core_2.13:4.0.0 \
  target/scala-2.13/data-engineering-project-assembly-0.1.0.jar \
  s3://my-bucket/bronze/weather \
  s3://my-bucket/silver/weather \
  2024-01-15
```

## Data Quality Rules

### Temperature Validation
- **Range**: -90°C to 60°C
- **Null Handling**: Reject records with null temperature
- **Outlier Detection**: Flag extreme values

### Humidity Validation
- **Range**: 0-100%
- **Null Handling**: Reject records with null humidity
- **Business Rule**: Must be percentage value

### Pressure Validation
- **Range**: 800-1100 hPa
- **Null Handling**: Reject records with null pressure
- **Atmospheric**: Standard atmospheric pressure range

### Wind Speed Validation
- **Range**: 0-100 m/s
- **Data Cleaning**: Remove non-numeric characters
- **Null Handling**: Default to 0.0

### Visibility Validation
- **Range**: 0-50 km
- **Null Handling**: Reject records with null visibility
- **Business Rule**: Must be positive value

## Silver Layer Schema

### Core Weather Data
- `temperature` (Double): Temperature in Celsius
- `wind_speed` (Double): Wind speed in m/s
- `city` (String): City name (cleaned and normalized)
- `timestamp` (Timestamp): Original data timestamp
- `humidity` (Double): Humidity percentage
- `pressure` (Double): Atmospheric pressure in hPa
- `visibility` (Double): Visibility in km

### Processing Metadata
- `processing_timestamp` (Timestamp): When data was processed
- `hour` (Integer): Hour of processing
- `minute` (Integer): Minute of processing
- `data_source` (String): Source system (noaa)

### Partitioning Columns
- `year` (Integer): Processing year
- `month` (Integer): Processing month
- `day` (Integer): Processing day

### Quality & Business Logic
- `quality_score` (Double): 0.0-1.0 quality score
- `is_valid` (Boolean): Overall validation flag
- `weather_category` (String): Business category
- `data_freshness_hours` (Double): Data age in hours

### Kafka Metadata
- `kafka_offset` (Long): Kafka offset
- `kafka_partition` (Integer): Kafka partition
- `kafka_timestamp` (Timestamp): Kafka timestamp

## Data Quality Metrics

The job provides comprehensive data quality metrics:

```
📊 Data Quality Metrics:
   Total Records: 1,234
   Valid Records: 1,180
   Invalid Records: 54
   Average Quality Score: 0.847
   Data Quality Rate: 95.6%
```

### Quality Score Calculation
```scala
quality_score = (
  temperature_valid + humidity_valid + pressure_valid + 
  wind_speed_valid + visibility_valid
) / 5.0
```

## Weather Categories

### Business Logic
- **HOT**: Temperature > 30°C
- **COLD**: Temperature < 0°C
- **WINDY**: Wind speed > 15 m/s
- **FOGGY**: Visibility < 5 km
- **NORMAL**: All other conditions

## Performance Tuning

### Spark Configuration
```bash
--driver-memory 4g
--executor-memory 8g
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.adaptive.coalescePartitions.enabled=true
--conf spark.sql.adaptive.skewJoin.enabled=true
--conf spark.sql.adaptive.localShuffleReader.enabled=true
```

### Delta Lake Optimizations
- **Auto-compaction**: Automatic file compaction
- **Optimize Write**: Optimized write operations
- **Z-ordering**: Better query performance
- **Vacuum**: Clean up old files

## Monitoring & Observability

### Data Quality Monitoring
- Record counts per validation rule
- Quality score distribution
- Processing time metrics
- Error rates and patterns

### Business Metrics
- Weather category distribution
- Data freshness trends
- Source system performance
- Geographic coverage

## Error Handling

### Validation Failures
- Invalid data ranges
- Missing required fields
- Data type mismatches
- Outlier detection

### Processing Errors
- S3 connectivity issues
- Delta Lake transaction failures
- Memory and resource constraints
- Network timeouts

## Integration with Great Expectations

The silver layer data can be validated using Great Expectations:

```bash
# Validate silver layer data
python great_expectations/simple_validation.py s3 s3://my-bucket/silver/weather
```

### Validation Rules
- Column presence and types
- Data range validations
- Business rule compliance
- Quality score ranges
- Weather category values

## Troubleshooting

### Common Issues

1. **S3 Access Denied**
   - Check AWS credentials
   - Verify bucket permissions
   - Ensure IAM policies are correct

2. **Delta Lake Errors**
   - Check Delta Lake version compatibility
   - Verify table schema
   - Ensure proper partitioning

3. **Memory Issues**
   - Increase driver/executor memory
   - Optimize data partitioning
   - Use adaptive query execution

4. **Data Quality Issues**
   - Review validation rules
   - Check source data quality
   - Adjust quality thresholds

### Debug Commands
```bash
# Check S3 data
aws s3 ls s3://my-bucket/bronze/weather/ --recursive

# Validate data
python great_expectations/simple_validation.py s3 s3://my-bucket/silver/weather

# Check Delta Lake table
spark.sql("DESCRIBE DETAIL delta.`s3://my-bucket/silver/weather`")
```

## Best Practices

### Data Quality
- Regular validation runs
- Quality score monitoring
- Automated alerting for failures
- Data lineage tracking

### Performance
- Optimize partitioning strategy
- Regular table optimization
- Monitor resource usage
- Use adaptive query execution

### Operations
- Automated scheduling
- Error handling and retry logic
- Monitoring and alerting
- Documentation and runbooks