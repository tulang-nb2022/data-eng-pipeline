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

echo "Build successful! Starting transformation job..."

# Set environment variables as needed

# Check if using Kafka or file input
if [ "$1" = "kafka" ]; then
  # Kafka streaming mode
  KAFKA_TOPIC=${2:-"weather-forecast"}
  SOURCE_TYPE=${3:-"noaa"}
  OUTPUT_PATH=${4:-"data/processed"}
  
  echo "Using Kafka streaming mode"
  echo "Kafka topic: $KAFKA_TOPIC"
  echo "Source type: $SOURCE_TYPE"
  echo "Output path: $OUTPUT_PATH"
  
  # Run the transformation job with Kafka
  $SPARK_HOME/bin/spark-submit \
    --class transform.DataTransformerApp \
    --master local[*] \
    --driver-memory 1g \
    --executor-memory 2g \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 \
    target/scala-2.12/data-engineering-project-assembly-0.1.0.jar \
    "$KAFKA_TOPIC" \
    "$SOURCE_TYPE" \
    "$OUTPUT_PATH"
else
  # File-based batch mode (for testing)
  INPUT_PATH=${1:-"data/raw"}
  SOURCE_TYPE=${2:-"noaa"}
  OUTPUT_PATH=${3:-"data/processed"}
  
  echo "Using file-based batch mode"
  echo "Input path: $INPUT_PATH"
  echo "Source type: $SOURCE_TYPE"
  echo "Output path: $OUTPUT_PATH"
  
  # Run the transformation job with files
  $SPARK_HOME/bin/spark-submit \
    --class transform.DataTransformerApp \
    --master local[*] \
    --driver-memory 1g \
    --executor-memory 2g \
    target/scala-2.12/data-engineering-project-assembly-0.1.0.jar \
    "$INPUT_PATH" \
    "$SOURCE_TYPE" \
    "$OUTPUT_PATH"
fi

echo "Transformation complete!" 