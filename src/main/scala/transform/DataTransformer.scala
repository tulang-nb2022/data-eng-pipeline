package transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.{Trigger, OutputMode, StreamingQuery}
import com.typesafe.config.{Config, ConfigFactory}
import scala.util.{Try, Success, Failure}

trait DataTransformer {
  protected lazy val config: Config = ConfigFactory.load()
  
  def transform(df: DataFrame): DataFrame
  
  // Modern streaming transform with better error handling
  def streamingTransform(df: DataFrame): DataFrame = {
    df.withColumn("processing_timestamp", current_timestamp())
      .withColumn("year", year(col("processing_timestamp")))
      .withColumn("month", month(col("processing_timestamp")))
      .withColumn("day", dayofmonth(col("processing_timestamp")))
      .withColumn("hour", hour(col("processing_timestamp")))
      .withColumn("minute", minute(col("processing_timestamp")))
  }
}

class NOAADataTransformer extends DataTransformer {
  override def transform(df: DataFrame): DataFrame = {
    df.withColumn("processing_timestamp", current_timestamp())
      .withColumn("year", year(col("processing_timestamp")))
      .withColumn("month", month(col("processing_timestamp")))
      .withColumn("day", dayofmonth(col("processing_timestamp")))
      .withColumn("data_source", lit("noaa"))
      .withColumn("data_version", lit("4.0.0"))
  }
}

class AlphaVantageDataTransformer extends DataTransformer {
  override def transform(df: DataFrame): DataFrame = {
    df.withColumn("processing_timestamp", current_timestamp())
      .withColumn("year", year(col("processing_timestamp")))
      .withColumn("month", month(col("processing_timestamp")))
      .withColumn("day", dayofmonth(col("processing_timestamp")))
      .withColumn("data_source", lit("alphavantage"))
      .withColumn("data_version", lit("4.0.0"))
  }
}

class EOSDISDataTransformer extends DataTransformer {
  override def transform(df: DataFrame): DataFrame = {
    df.withColumn("processing_timestamp", current_timestamp())
      .withColumn("year", year(col("processing_timestamp")))
      .withColumn("month", month(col("processing_timestamp")))
      .withColumn("day", dayofmonth(col("processing_timestamp")))
      .withColumn("data_source", lit("eosdis"))
      .withColumn("data_version", lit("4.0.0"))
  }
}

// Main application object with modern error handling
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
    // Create Spark session with modern configuration
    val spark = SparkSession.builder()
      .appName("Data Engineering Project - Spark 4.0.0")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.json.allowBackslashEscapingAnyCharacter", "true")
      .config("spark.sql.json.ignoreNullFields", "false")
      .config("spark.sql.streaming.checkpointLocation", "data/checkpoint")
      .getOrCreate()

    // Set log level
    spark.sparkContext.setLogLevel("WARN")

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
      
      Try {
        val kafkaStream = spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", topic)
          .option("startingOffsets", "earliest")
          .option("failOnDataLoss", "false")
          .option("maxOffsetsPerTrigger", 1000)
          .load()

        // Enhanced JSON schema for Spark 4.0.0
        val jsonSchema = StructType(Seq(
          StructField("temperature", DoubleType, true),
          StructField("wind_speed", StringType, true),
          StructField("city", StringType, true),
          StructField("timestamp", StringType, true),
          StructField("humidity", DoubleType, true),
          StructField("pressure", DoubleType, true),
          StructField("visibility", DoubleType, true)
        ))

        // Parse JSON with enhanced error handling
        val parsedStream = kafkaStream
          .selectExpr("CAST(value AS STRING) as json_data")
          .filter(col("json_data").isNotNull && length(col("json_data")) > 0)
          .select(from_json(col("json_data"), jsonSchema).as("data"))
          .select("data.*")
          .na.fill(0) // Handle null values

        parsedStream
      } match {
        case Success(df) => df
        case Failure(e) =>
          println(s"Error creating Kafka stream: ${e.getMessage}")
          throw e
      }
    }

    try {
      println(s"Starting Spark 4.0.0 streaming job...")
      println(s"Topic: $kafkaTopic")
      println(s"Source type: $sourceType")
      println(s"Output path: $outputPath")
      println(s"Checkpoint path: $checkpointPath")

      val transformer = DataTransformerApp(sourceType)(spark)
      
      // Create Kafka stream
      val kafkaStream = createKafkaStream(kafkaTopic)
      
      // Apply streaming transformation
      val transformedStream = kafkaStream.transform(transformer.streamingTransform)
      
      // Start the streaming query with modern configuration
      val query = transformedStream.writeStream
        .format("parquet")
        .outputMode(OutputMode.Append)
        .option("checkpointLocation", checkpointPath)
        .option("path", outputPath)
        .partitionBy("year", "month", "day")
        .trigger(Trigger.ProcessingTime("30 seconds"))
        .queryName(s"${sourceType}_streaming_query")
        .start()
      
      println("Streaming query started successfully!")
      println("Press Ctrl+C to stop...")
      
      // Add graceful shutdown
      sys.addShutdownHook {
        println("Shutting down gracefully...")
        query.stop()
        spark.stop()
      }
      
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