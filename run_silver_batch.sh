#!/bin/bash

# Silver Layer Batch Job Runner
# Standalone Spark batch job for data quality and enrichment

# Add assembly plugin to project/plugins.sbt if it doesn't exist
if [ ! -f "project/plugins.sbt" ]; then
  mkdir -p project
  echo 'addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.1")' > project/plugins.sbt
fi

# Build the Scala project
echo "Building Silver Layer Batch Job with Spark 4.0.0 and Delta Lake..."
sbt clean assembly

# Check if build was successful
if [ $? -ne 0 ]; then
  echo "Build failed!"
  exit 1
fi

echo "Build successful! Starting Silver Layer Batch Job..."

# Set default values
BRONZE_S3_PATH=${1:-"s3://your-data-bucket/bronze/weather"}
SILVER_S3_PATH=${2:-"s3://your-data-bucket/silver/weather"}
PROCESSING_DATE=${3:-$(date +%Y-%m-%d)}

echo "=========================================="
echo "Silver Layer Batch Job Configuration"
echo "=========================================="
echo "Bronze S3 Path: $BRONZE_S3_PATH"
echo "Silver S3 Path: $SILVER_S3_PATH"
echo "Processing Date: $PROCESSING_DATE"
echo "=========================================="

# Spark configuration optimized for batch processing
SPARK_CONF=(
  "--master" "local[*]"
  "--driver-memory" "4g"
  "--executor-memory" "8g"
  "--conf" "spark.driver.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.misc=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED"
  "--conf" "spark.executor.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.misc=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED"
  "--conf" "spark.sql.adaptive.enabled=true"
  "--conf" "spark.sql.adaptive.coalescePartitions.enabled=true"
  "--conf" "spark.sql.adaptive.skewJoin.enabled=true"
  "--conf" "spark.sql.adaptive.localShuffleReader.enabled=true"
  "--conf" "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
  "--conf" "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
  "--conf" "spark.serializer=org.apache.spark.serializer.KryoSerializer"
  "--conf" "spark.sql.adaptive.advisoryPartitionSizeInBytes=128MB"
  "--conf" "spark.sql.adaptive.maxRecordsPerPartition=1000000"
)

# Delta Lake package
PACKAGES="io.delta:delta-core_2.13:4.0.0"

# Run the silver layer batch job
echo "Starting Silver Layer Batch Job..."
$SPARK_HOME/bin/spark-submit \
  "${SPARK_CONF[@]}" \
  --packages "$PACKAGES" \
  --class batch.SilverLayerBatchJob \
  target/scala-2.13/data-engineering-project-assembly-0.1.0.jar \
  "$BRONZE_S3_PATH" \
  "$SILVER_S3_PATH" \
  "$PROCESSING_DATE"

echo "Silver Layer Batch Job completed!"