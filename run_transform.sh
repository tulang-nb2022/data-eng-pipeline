#!/bin/bash

# Build the Scala project
echo "Building Scala project..."
sbt clean assembly

# Set environment variables
export SPARK_HOME=/path/to/spark
export HADOOP_CONF_DIR=/path/to/hadoop/conf

# Run the transformation job
echo "Running transformation job..."
$SPARK_HOME/bin/spark-submit \
  --class transform.TransformJob \
  --master local[*] \
  --driver-memory 4g \
  --executor-memory 4g \
  target/scala-2.12/data-engineering-project-assembly-0.1.0.jar \
  "s3://your-bucket/raw/market_data" \
  "alphavantage" \
  "s3://your-bucket/processed/market_data"

echo "Transformation complete!" 