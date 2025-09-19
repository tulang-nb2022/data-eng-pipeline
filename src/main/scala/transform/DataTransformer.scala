package transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.{Trigger, OutputMode, StreamingQuery}
import org.apache.spark.sql.expressions.Window
import com.typesafe.config.{Config, ConfigFactory}
import scala.util.{Try, Success, Failure}

// Bronze Layer: Raw data ingestion from Kafka
object BronzeLayerTransformer {
  
  def createBronzeSchema(): StructType = {
    StructType(Seq(
      StructField("temperature", DoubleType, true),
      StructField("wind_speed", StringType, true),
      StructField("city", StringType, true),
      StructField("timestamp", StringType, true),
      StructField("humidity", DoubleType, true),
      StructField("pressure", DoubleType, true),
      StructField("visibility", DoubleType, true),
      StructField("processing_timestamp", TimestampType, true),
      StructField("hour", IntegerType, true),
      StructField("minute", IntegerType, true),
      StructField("kafka_offset", LongType, true),
      StructField("kafka_partition", IntegerType, true),
      StructField("kafka_timestamp", TimestampType, true),
      StructField("data_source", StringType, true)
    ))
  }
  
  def transformToBronze(df: DataFrame, dataSource: String): DataFrame = {
    df.withColumn("processing_timestamp", current_timestamp())
      .withColumn("hour", hour(col("processing_timestamp")))
      .withColumn("minute", minute(col("processing_timestamp")))
      .withColumn("kafka_offset", col("offset"))
      .withColumn("kafka_partition", col("partition"))
      .withColumn("kafka_timestamp", col("kafka_timestamp"))
      .withColumn("data_source", lit(dataSource))
      .drop("offset", "partition")
  }
}

// Silver Layer: Cleaned and enriched data
object SilverLayerTransformer {
  
  def createSilverSchema(): StructType = {
    StructType(Seq(
      StructField("temperature", DoubleType, false),
      StructField("wind_speed", DoubleType, false),
      StructField("city", StringType, false),
      StructField("timestamp", TimestampType, false),
      StructField("humidity", DoubleType, false),
      StructField("pressure", DoubleType, false),
      StructField("visibility", DoubleType, false),
      StructField("processing_timestamp", TimestampType, false),
      StructField("hour", IntegerType, false),
      StructField("minute", IntegerType, false),
      StructField("data_source", StringType, false),
      StructField("year", IntegerType, false),
      StructField("month", IntegerType, false),
      StructField("day", IntegerType, false),
      StructField("quality_score", DoubleType, false),
      StructField("is_valid", BooleanType, false),
      StructField("weather_category", StringType, false),
      StructField("data_freshness_hours", DoubleType, false),
      StructField("kafka_offset", LongType, false),
      StructField("kafka_partition", IntegerType, false),
      StructField("kafka_timestamp", TimestampType, false)
    ))
  }
  
  def transformToSilver(df: DataFrame): DataFrame = {
    df
      // Step 1: Data type conversions and cleaning
      .withColumn("wind_speed_cleaned", 
        when(col("wind_speed").isNull, 0.0)
        .otherwise(
          regexp_replace(col("wind_speed"), "[^0-9.]", "").cast(DoubleType)
        )
      )
      .withColumn("timestamp_cleaned", 
        when(col("timestamp").isNull, col("processing_timestamp"))
        .otherwise(
          coalesce(
            try_to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),  // With microseconds
            try_to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS"),     // With milliseconds
            try_to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"),         // Without subseconds
            try_to_timestamp(col("timestamp")),                                  // ISO format auto-parse
            col("processing_timestamp")                                          // Fallback
          )
        )
      )
      .withColumn("city_cleaned",
        when(col("city").isNull, lit("UNKNOWN"))
        .otherwise(trim(upper(col("city"))))
      )
      
      // Step 2: Data validation and outlier detection
      .withColumn("temperature_valid", 
        col("temperature").isNotNull && 
        col("temperature") >= -90 && 
        col("temperature") <= 60
      )
      .withColumn("humidity_valid", 
        col("humidity").isNotNull && 
        col("humidity") >= 0 && 
        col("humidity") <= 100
      )
      .withColumn("pressure_valid", 
        col("pressure").isNotNull && 
        col("pressure") >= 800 && 
        col("pressure") <= 1100
      )
      .withColumn("wind_speed_valid", 
        col("wind_speed_cleaned") >= 0 && 
        col("wind_speed_cleaned") <= 100
      )
      .withColumn("visibility_valid", 
        col("visibility").isNotNull && 
        col("visibility") >= 0 && 
        col("visibility") <= 50
      )
      
      // Step 3: Data quality scoring (more sophisticated)
      .withColumn("quality_score", 
        (
          when(col("temperature_valid"), 1.0).otherwise(0.0) +
          when(col("humidity_valid"), 1.0).otherwise(0.0) +
          when(col("pressure_valid"), 1.0).otherwise(0.0) +
          when(col("wind_speed_valid"), 1.0).otherwise(0.0) +
          when(col("visibility_valid"), 1.0).otherwise(0.0)
        ) / 5.0
      )
      
      // Step 4: Overall validation flag
      .withColumn("is_valid", 
        col("temperature_valid") && 
        col("humidity_valid") && 
        col("pressure_valid") &&
        col("wind_speed_valid") &&
        col("visibility_valid")
      )
      
      // Step 5: Add business logic columns
      .withColumn("weather_category",
        when(col("temperature") > 30, "HOT")
        .when(col("temperature") < 0, "COLD")
        .when(col("wind_speed_cleaned") > 15, "WINDY")
        .when(col("visibility") < 5, "FOGGY")
        .otherwise("NORMAL")
      )
      .withColumn("data_freshness_hours",
        (unix_timestamp(col("processing_timestamp")) - unix_timestamp(col("timestamp_cleaned"))) / 3600
      )
      
      // Step 6: Add date partitioning columns
      .withColumn("year", year(col("processing_timestamp")))
      .withColumn("month", month(col("processing_timestamp")))
      .withColumn("day", dayofmonth(col("processing_timestamp")))
      
      // Step 7: Deduplicate based on key fields (keep latest)
      .withColumn("row_number", 
        row_number().over(
          Window.partitionBy("temperature", "wind_speed_cleaned", "city_cleaned", "timestamp_cleaned", "data_source")
          .orderBy(col("processing_timestamp").desc)
        )
      )
      .filter(col("row_number") === 1)
      .drop("row_number")
      
      // Step 8: Final schema enforcement and column selection
      .select(
        col("temperature").cast(DoubleType).as("temperature"),
        col("wind_speed_cleaned").cast(DoubleType).as("wind_speed"),
        col("city_cleaned").cast(StringType).as("city"),
        col("timestamp_cleaned").cast(TimestampType).as("timestamp"),
        col("humidity").cast(DoubleType).as("humidity"),
        col("pressure").cast(DoubleType).as("pressure"),
        col("visibility").cast(DoubleType).as("visibility"),
        col("processing_timestamp").cast(TimestampType).as("processing_timestamp"),
        col("hour").cast(IntegerType).as("hour"),
        col("minute").cast(IntegerType).as("minute"),
        col("data_source").cast(StringType).as("data_source"),
        col("year").cast(IntegerType).as("year"),
        col("month").cast(IntegerType).as("month"),
        col("day").cast(IntegerType).as("day"),
        col("quality_score").cast(DoubleType).as("quality_score"),
        col("is_valid").cast(BooleanType).as("is_valid"),
        col("weather_category").cast(StringType).as("weather_category"),
        col("data_freshness_hours").cast(DoubleType).as("data_freshness_hours"),
        col("kafka_offset").cast(LongType).as("kafka_offset"),
        col("kafka_partition").cast(IntegerType).as("kafka_partition"),
        col("kafka_timestamp").cast(TimestampType).as("kafka_timestamp")
      )
  }
}

// Legacy transformers for backward compatibility
trait DataTransformer {
  protected lazy val config: Config = ConfigFactory.load()
  def transform(df: DataFrame): DataFrame
}

class NOAADataTransformer extends DataTransformer {
  override def transform(df: DataFrame): DataFrame = {
    BronzeLayerTransformer.transformToBronze(df, "noaa")
  }
}


// Main application object with data lakehouse architecture
object DataTransformerApp {
  
  def apply(source: String)(implicit spark: SparkSession): DataTransformer = {
    source.toLowerCase match {
      case "noaa" => new NOAADataTransformer
      case _ => throw new IllegalArgumentException(s"Unknown source: $source. Supported sources: noaa")
    }
  }

  def main(args: Array[String]): Unit = {
    // Create Spark session with Delta Lake configuration
    val spark = SparkSession.builder()
      .appName("Data Lakehouse Pipeline - Spark 3.5.0")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.json.allowBackslashEscapingAnyCharacter", "true")
      .config("spark.sql.json.ignoreNullFields", "false")
      .config("spark.sql.streaming.checkpointLocation", "data/checkpoint")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    // Set log level
    spark.sparkContext.setLogLevel("WARN")

    if (args.length < 4) {
      println("Usage: DataTransformerApp <mode> <kafka_topic> <source_type> <bronze_s3_path> [silver_s3_path]")
      println("Modes: bronze, silver")
      println("source_type options: noaa")
      println("Note: Gold layer should be run using dbt")
      System.exit(1)
    }

    val mode = args(0)
    val kafkaTopic = args(1)
    val sourceType = args(2)
    val bronzeS3Path = args(3)
    val silverS3Path = if (args.length > 4) args(4) else bronzeS3Path.replace("/bronze/", "/silver/")
    val goldS3Path = if (args.length > 5) args(5) else bronzeS3Path.replace("/bronze/", "/gold/")

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

        // JSON schema matching the actual data structure
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
          .selectExpr("CAST(value AS STRING) as json_data", "offset", "partition", "timestamp as kafka_timestamp")
          .filter(col("json_data").isNotNull && length(col("json_data")) > 0)
          .select(from_json(col("json_data"), jsonSchema).as("data"), col("offset"), col("partition"), col("kafka_timestamp"))
          .select("data.*", "offset", "partition", "kafka_timestamp")
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
      println(s"Starting Data Lakehouse Pipeline...")
      println(s"Mode: $mode")
      println(s"Topic: $kafkaTopic")
      println(s"Source type: $sourceType")
      println(s"Bronze S3 path: $bronzeS3Path")
      println(s"Silver S3 path: $silverS3Path")
      println(s"Gold S3 path: $goldS3Path")

      mode.toLowerCase match {
        case "bronze" => runBronzeLayer(spark, kafkaTopic, sourceType, bronzeS3Path)
        case "silver" => runSilverLayer(spark, bronzeS3Path, silverS3Path)
        case _ => 
          println(s"Unknown mode: $mode. Use: bronze or silver")
          println("Note: Gold layer should be run using dbt, not Spark")
          System.exit(1)
      }
      
    } catch {
      case e: Exception =>
        println(s"Error during transformation: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
    }
  }

  def runBronzeLayer(spark: SparkSession, kafkaTopic: String, sourceType: String, bronzeS3Path: String): Unit = {
    println("Running Bronze Layer - Raw data ingestion...")
    
    val transformer = DataTransformerApp(sourceType)(spark)
    
    // Create Kafka stream using the local function defined in main
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", 1000)
      .load()

    // JSON schema matching the actual data structure
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
      .selectExpr("CAST(value AS STRING) as json_data", "offset", "partition", "timestamp as kafka_timestamp")
      .filter(col("json_data").isNotNull && length(col("json_data")) > 0)
      .select(from_json(col("json_data"), jsonSchema).as("data"), col("offset"), col("partition"), col("kafka_timestamp"))
      .select("data.*", "offset", "partition", "kafka_timestamp")
      .na.fill(0) // Handle null values
    
    val bronzeStream = BronzeLayerTransformer.transformToBronze(parsedStream, sourceType)
    
    val query = bronzeStream.writeStream
      .format("delta")
      .outputMode(OutputMode.Append)
      .option("checkpointLocation", s"data/checkpoint/bronze_$sourceType")
      .option("path", bronzeS3Path)
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .queryName(s"bronze_${sourceType}_streaming_query")
      .start()
    
    println("Bronze layer streaming query started!")
    println("Press Ctrl+C to stop...")
    
    sys.addShutdownHook {
      println("Shutting down Bronze layer...")
      query.stop()
    }
    
    query.awaitTermination()
  }

  def runSilverLayer(spark: SparkSession, bronzeS3Path: String, silverS3Path: String): Unit = {
    println("Running Silver Layer - Data cleaning and enrichment...")
    
    val bronzeData = spark.read.format("delta").load(bronzeS3Path)
    val silverData = SilverLayerTransformer.transformToSilver(bronzeData)
    
    // Add data quality metrics
    val qualityMetrics = silverData
      .agg(
        count("*").as("total_records"),
        count(when(col("is_valid") === true, 1)).as("valid_records"),
        count(when(col("is_valid") === false, 1)).as("invalid_records"),
        avg("quality_score").as("avg_quality_score"),
        min("processing_timestamp").as("earliest_record"),
        max("processing_timestamp").as("latest_record")
      )
    
    println("Data Quality Metrics:")
    qualityMetrics.show()
    
    // Write silver data with optimizations
    silverData.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .option("delta.autoOptimize.optimizeWrite", "true")
      .option("delta.autoOptimize.autoCompact", "true")
      .partitionBy("year", "month", "day")
      .save(silverS3Path)
    
    // Optimize the table for better query performance
    spark.sql(s"OPTIMIZE delta.`$silverS3Path`")
    
    println(s"Silver layer completed! Data saved to: $silverS3Path")
    println("Table optimized for better query performance.")
  }


} 