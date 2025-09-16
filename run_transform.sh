#!/bin/bash

# Data Lakehouse Pipeline Runner
# Supports Bronze, Silver, and Gold layers with Delta Lake

# Add assembly plugin to project/plugins.sbt if it doesn't exist
if [ ! -f "project/plugins.sbt" ]; then
  mkdir -p project
  echo 'addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.1")' > project/plugins.sbt
fi

# Build the Scala project
echo "Building Data Lakehouse Pipeline with Spark 3.5.0 and Delta Lake..."
sbt clean assembly

# Check if build was successful
if [ $? -ne 0 ]; then
  echo "Build failed!"
  exit 1
fi

echo "Build successful! Starting Data Lakehouse Pipeline..."

# Set default values
MODE=${1:-"bronze"}
KAFKA_TOPIC=${2:-"weather-forecast"}
SOURCE_TYPE=${3:-"noaa"}
BRONZE_S3_PATH=${4:-"s3://your-data-bucket/bronze/weather"}
SILVER_S3_PATH=${5:-"s3://your-data-bucket/silver/weather"}
GOLD_S3_PATH=${6:-"s3://your-data-bucket/gold/weather"}

echo "=========================================="
echo "Data Lakehouse Pipeline Configuration"
echo "=========================================="
echo "Mode: $MODE"
echo "Kafka Topic: $KAFKA_TOPIC"
echo "Source Type: $SOURCE_TYPE"
echo "Bronze S3 Path: $BRONZE_S3_PATH"
echo "Silver S3 Path: $SILVER_S3_PATH"
echo "Gold S3 Path: $GOLD_S3_PATH"
echo "=========================================="

# Common Spark configuration
SPARK_CONF=(
  "--master" "local[*]"
  "--driver-memory" "2g"
  "--executor-memory" "4g"
  "--conf" "spark.driver.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.misc=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED"
  "--conf" "spark.executor.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.misc=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED"
  "--conf" "spark.sql.adaptive.enabled=true"
  "--conf" "spark.sql.adaptive.coalescePartitions.enabled=true"
  "--conf" "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
  "--conf" "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
  "--conf" "spark.sql.streaming.checkpointLocation=data/checkpoint"
)

# Delta Lake and Kafka packages
PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,io.delta:delta-core_2.13:2.4.0"

# Run the appropriate layer
case $MODE in
  "bronze")
    echo "Starting Bronze Layer - Raw data ingestion from Kafka..."
    $SPARK_HOME/bin/spark-submit \
      "${SPARK_CONF[@]}" \
      --packages "$PACKAGES" \
      --class transform.DataTransformerApp \
      target/scala-2.13/data-engineering-project-assembly-0.1.0.jar \
      "$MODE" \
      "$KAFKA_TOPIC" \
      "$SOURCE_TYPE" \
      "$BRONZE_S3_PATH" \
      "$SILVER_S3_PATH" \
      "$GOLD_S3_PATH"
    ;;
    
  "silver")
    echo "Starting Silver Layer - Data cleaning and enrichment..."
    $SPARK_HOME/bin/spark-submit \
      "${SPARK_CONF[@]}" \
      --packages "io.delta:delta-core_2.13:2.4.0" \
      --class transform.DataTransformerApp \
      target/scala-2.13/data-engineering-project-assembly-0.1.0.jar \
      "$MODE" \
      "$KAFKA_TOPIC" \
      "$SOURCE_TYPE" \
      "$BRONZE_S3_PATH" \
      "$SILVER_S3_PATH" \
      "$GOLD_S3_PATH"
    ;;
    
  "all")
    echo "Running complete pipeline: Bronze -> Silver"
    echo "Note: Gold layer should be run separately using dbt"
    
    echo "Step 1: Bronze Layer..."
    $SPARK_HOME/bin/spark-submit \
      "${SPARK_CONF[@]}" \
      --packages "$PACKAGES" \
      --class transform.DataTransformerApp \
      target/scala-2.13/data-engineering-project-assembly-0.1.0.jar \
      "bronze" \
      "$KAFKA_TOPIC" \
      "$SOURCE_TYPE" \
      "$BRONZE_S3_PATH" \
      "$SILVER_S3_PATH" &
    
    BRONZE_PID=$!
    sleep 30  # Let bronze layer run for 30 seconds
    
    echo "Step 2: Silver Layer..."
    $SPARK_HOME/bin/spark-submit \
      "${SPARK_CONF[@]}" \
      --packages "io.delta:delta-core_2.13:2.4.0" \
      --class transform.DataTransformerApp \
      target/scala-2.13/data-engineering-project-assembly-0.1.0.jar \
      "silver" \
      "$KAFKA_TOPIC" \
      "$SOURCE_TYPE" \
      "$BRONZE_S3_PATH" \
      "$SILVER_S3_PATH"
    
    # Stop bronze layer
    kill $BRONZE_PID 2>/dev/null
    
    echo ""
    echo "✅ Spark pipeline completed!"
    echo "📊 Next step: Run dbt for gold layer"
    echo "   dbt run --models gold"
    ;;
    
  *)
    echo "Usage: $0 <mode> [kafka_topic] [source_type] [bronze_s3_path] [silver_s3_path]"
    echo ""
    echo "Modes:"
    echo "  bronze  - Raw data ingestion from Kafka to S3 (streaming)"
    echo "  silver  - Data cleaning and enrichment (batch)"
    echo "  all     - Run complete pipeline (bronze + silver)"
    echo ""
    echo "Examples:"
    echo "  $0 bronze weather-forecast noaa s3://my-bucket/bronze/weather"
    echo "  $0 silver weather-forecast noaa s3://my-bucket/bronze/weather s3://my-bucket/silver/weather"
    echo "  $0 all weather-forecast noaa s3://my-bucket/bronze/weather s3://my-bucket/silver/weather"
    echo ""
    echo "Note: Gold layer should be run using dbt:"
    echo "  dbt run --models gold"
    exit 1
    ;;
esac

echo "Data Lakehouse Pipeline completed!" 