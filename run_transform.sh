#!/bin/bash

# Add assembly plugin to project/plugins.sbt if it doesn't exist
if [ ! -f "project/plugins.sbt" ]; then
  mkdir -p project
  echo 'addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.2.0")' > project/plugins.sbt
fi

# Build the Scala project
echo "Building Scala project..."
sbt clean assembly

# Check if build was successful
if [ $? -ne 0 ]; then
  echo "Build failed!"
  exit 1
fi

echo "Build successful! Running transformation job..."

# Set environment variables
export SPARK_HOME=/opt/spark
# export HADOOP_CONF_DIR=/path/to/hadoop/conf

# Run the transformation job
$SPARK_HOME/bin/spark-submit \
  --class transform.DataTransformerApp \
  --master local[*] \
  --driver-memory 4g \
  --executor-memory 4g \
  --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-s3:1.12.261 \
  target/scala-2.12/data-engineering-project-assembly-0.1.0.jar \
  "s3://data-eng-bucket-345/weather-forecast/raw" \
  "noaa" \
  "s3://data-eng-bucket-345/weather-forecast/processed"

echo "Transformation complete!" 