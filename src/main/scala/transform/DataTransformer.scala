package transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.{Trigger, OutputMode, StreamingQuery}
import com.typesafe.config.{Config, ConfigFactory}

trait DataTransformer {
  protected lazy val config: Config = ConfigFactory.load()
  
  def transform(df: DataFrame): DataFrame
  
  // Simple streaming transform - just add timestamp and basic fields
  def streamingTransform(df: DataFrame): DataFrame = {
    df.withColumn("processing_timestamp", current_timestamp())
      .withColumn("year", year(col("processing_timestamp")))
      .withColumn("month", month(col("processing_timestamp")))
      .withColumn("day", dayofmonth(col("processing_timestamp")))
  }
}

class NOAADataTransformer extends DataTransformer {
  override def transform(df: DataFrame): DataFrame = {
    // Simple transformation - just add basic fields
    df.withColumn("processing_timestamp", current_timestamp())
      .withColumn("year", year(col("processing_timestamp")))
      .withColumn("month", month(col("processing_timestamp")))
      .withColumn("day", dayofmonth(col("processing_timestamp")))
      .withColumn("data_source", lit("noaa"))
  }
}

class AlphaVantageDataTransformer extends DataTransformer {
  override def transform(df: DataFrame): DataFrame = {
    // Simple transformation for financial data
    df.withColumn("processing_timestamp", current_timestamp())
      .withColumn("year", year(col("processing_timestamp")))
      .withColumn("month", month(col("processing_timestamp")))
      .withColumn("day", dayofmonth(col("processing_timestamp")))
      .withColumn("data_source", lit("alphavantage"))
  }
}

class EOSDISDataTransformer extends DataTransformer {
  override def transform(df: DataFrame): DataFrame = {
    // Simple transformation for EOSDIS data
    df.withColumn("processing_timestamp", current_timestamp())
      .withColumn("year", year(col("processing_timestamp")))
      .withColumn("month", month(col("processing_timestamp")))
      .withColumn("day", dayofmonth(col("processing_timestamp")))
      .withColumn("data_source", lit("eosdis"))
  }
}

// Main application object
object DataTransformerApp {
  def apply(source: String)(implicit spark: SparkSession): DataTransformer = {
    source.toLowerCase match {
      case "noaa" => new NOAADataTransformer
      case "alphavantage" => new AlphaVantageDataTransformer
      case "eosdis" => new EOSDISDataTransformer
      case _ => throw new IllegalArgumentException(s"Unknown source: $source")
    }
  }

  def main(args: Array[String]): Unit = {
    // Create Spark session with Kafka support
    val spark = SparkSession.builder()
      .appName("Simple Data Transformation Job")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.json.allowBackslashEscapingAnyCharacter", "true")
      .config("spark.sql.json.ignoreNullFields", "false")
      .getOrCreate()

    if (args.length < 3) {
      println("Usage: DataTransformerApp <kafka_topic> <source_type> <output_path> [checkpoint_path]")
      println("source_type options: eosdis, alphavantage, noaa")
      System.exit(1)
    }

    val kafkaTopic = args(0)
    val sourceType = args(1)
    val outputPath = args(2)
    val checkpointPath = if (args.length > 3) args(3) else "data/checkpoint"

    def createKafkaStream(topic: String): DataFrame = {
      println(s"Creating Kafka stream from topic: $topic")
      
      val kafkaStream = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()

      // Parse JSON with a simple schema
      val parsedStream = kafkaStream
        .selectExpr("CAST(value AS STRING) as json_data")
        .filter(col("json_data").isNotNull && length(col("json_data")) > 0)
        .select(from_json(col("json_data"), 
          StructType(Seq(
            StructField("temperature", DoubleType, true),
            StructField("wind_speed", StringType, true),
            StructField("city", StringType, true),
            StructField("open", DoubleType, true),
            StructField("close", DoubleType, true),
            StructField("volume", LongType, true),
            StructField("score", DoubleType, true),
            StructField("timestamp", StringType, true)
          ))
        ).as("data"))
        .select("data.*")

      parsedStream
    }

    try {
      println(s"Starting simple Kafka streaming job...")
      println(s"Topic: $kafkaTopic")
      println(s"Source type: $sourceType")
      println(s"Output path: $outputPath")
      println(s"Checkpoint path: $checkpointPath")

      val transformer = DataTransformerApp(sourceType)(spark)
      
      // Create Kafka stream
      val kafkaStream = createKafkaStream(kafkaTopic)
      
      // Apply streaming transformation
      val transformedStream = kafkaStream.transform(transformer.streamingTransform)
      
      // Start the streaming query
      val query = transformedStream.writeStream
        .format("parquet")
        .outputMode(OutputMode.Append)
        .option("checkpointLocation", checkpointPath)
        .option("path", outputPath)
        .partitionBy("year", "month", "day")
        .trigger(Trigger.ProcessingTime("1 minute"))
        .start()
      
      println("Streaming query started successfully!")
      println("Press Ctrl+C to stop...")
      
      query.awaitTermination()
      
    } catch {
      case e: Exception =>
        println(s"Error during transformation: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
    }
  }
} 