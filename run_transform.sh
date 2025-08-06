#!/bin/bash

# Add assembly plugin to project/plugins.sbt if it doesn't exist
if [ ! -f "project/plugins.sbt" ]; then
  mkdir -p project
  echo 'addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.1")' > project/plugins.sbt
fi

# Build the Scala project
echo "Building Scala project with Spark 4.0.0 and Scala 2.13..."
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
  
  echo "Using Kafka streaming mode with Spark 4.0.0"
  echo "Kafka topic: $KAFKA_TOPIC"
  echo "Source type: $SOURCE_TYPE"
  echo "Output path: $OUTPUT_PATH"
  
  # Run the transformation job with Kafka
  $SPARK_HOME/bin/spark-submit \
    --class transform.DataTransformerApp \
    --master local[*] \
    --driver-memory 2g \
    --executor-memory 4g \
    --conf spark.driver.extraJavaOptions="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.misc=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED" \
    --conf spark.executor.extraJavaOptions="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.misc=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED" \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.sql.streaming.checkpointLocation=data/checkpoint \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
    target/scala-2.13/data-engineering-project-assembly-0.1.0.jar \
    "$KAFKA_TOPIC" \
    "$SOURCE_TYPE" \
    "$OUTPUT_PATH"
else
  # File-based batch mode (for testing)
  INPUT_PATH=${1:-"data/raw"}
  SOURCE_TYPE=${2:-"noaa"}
  OUTPUT_PATH=${3:-"data/processed"}
  
  echo "Using file-based batch mode with Spark 4.0.0"
  echo "Input path: $INPUT_PATH"
  echo "Source type: $SOURCE_TYPE"
  echo "Output path: $OUTPUT_PATH"
  
  # Run the transformation job with files
  $SPARK_HOME/bin/spark-submit \
    --class transform.DataTransformerApp \
    --master local[*] \
    --driver-memory 2g \
    --executor-memory 4g \
    --conf spark.driver.extraJavaOptions="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.misc=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED" \
    --conf spark.executor.extraJavaOptions="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.misc=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED" \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    target/scala-2.13/data-engineering-project-assembly-0.1.0.jar \
    "$INPUT_PATH" \
    "$SOURCE_TYPE" \
    "$OUTPUT_PATH"
fi

echo "Transformation complete!" 