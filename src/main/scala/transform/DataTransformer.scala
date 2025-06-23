package transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.{Trigger, OutputMode, StreamingQuery, StreamingQueryListener}
import org.apache.spark.sql.expressions.Window
import io.delta.tables._
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryStartedEvent, QueryProgressEvent, QueryTerminatedEvent}
import java.time.{Duration, Instant}
import scala.collection.mutable
import com.typesafe.config.{Config, ConfigFactory}

trait DataTransformer {
  protected lazy val config: Config = ConfigFactory.load()
  
  def transform(df: DataFrame): DataFrame
  
  def validateData(df: DataFrame): Unit = {
    // Check for null values
    val nullCounts = df.columns.map(c => (c, df.filter(col(c).isNull).count()))
    nullCounts.filter(_._2 > 0).foreach { case (column, count) =>
      println(s"Warning: Found $count null values in column $column")
    }
    
    // Check for duplicate rows
    val dupes = df.count() - df.distinct().count()
    if (dupes > 0) {
      println(s"Warning: Found $dupes duplicate rows")
    }
  }

  // Common streaming configuration
  protected def createStreamingQuery(
    inputStream: DataFrame,
    checkpointLocation: String = config.getString("storage.checkpoint_location"),
    outputPath: String = config.getString("storage.processed_data_path"),
    triggerInterval: String = "1 minute"
  ): StreamingQuery = {
    inputStream.writeStream
      .format("parquet")
      .outputMode(OutputMode.Append)
      .option("checkpointLocation", checkpointLocation)
      .option("path", outputPath)
      .option("mergeSchema", "true")
      .partitionBy("year", "month", "day")  // Partition by date for efficient querying
      .trigger(Trigger.ProcessingTime(triggerInterval))
      .start()
  }
}

class EOSDISTransformer extends DataTransformer {
  override def transform(df: DataFrame): DataFrame = {
    validateData(df)
    
    // Convert score to double and handle nulls
    val dfWithScore = df.withColumn("score", 
      when(col("score").cast(DoubleType).isNotNull, col("score").cast(DoubleType))
        .otherwise(lit(null))
    )
    
    // Create hierarchical categories from fields
    val dfWithCategories = dfWithScore
      .withColumn("category_hierarchy", 
        split(col("fields"), ":")
      )
      .withColumn("main_category",
        array_min(col("category_hierarchy"))
      )
    
    dfWithCategories
  }
}

class FinancialDataTransformer extends DataTransformer {
  override def transform(df: DataFrame): DataFrame = {
    validateData(df)
    
    // Convert string columns to numeric, removing '$' and ',' characters
    val numericDf = df.columns.foldLeft(df) { (accDf, colName) =>
      val schema = accDf.schema
      val field = schema.fields.find(_.name == colName).get
      if (field.dataType == StringType) {
        accDf.withColumn(colName,
          regexp_replace(regexp_replace(col(colName), "\\$", ""), ",", "").cast(DoubleType)
        )
      } else accDf
    }
    
    // Calculate daily returns
    val withReturns = numericDf
      .withColumn("daily_return",
        (col("close") - lag("close", 1).over(Window.orderBy("date"))) / 
        lag("close", 1).over(Window.orderBy("date"))
      )
    
    // Calculate 20-day rolling volatility
    val withVolatility = withReturns
      .withColumn("volatility",
        stddev("daily_return")
          .over(Window.orderBy("date").rowsBetween(-19, 0))
      )
    
    // Mark outliers (returns more than 3 std dev from mean)
    val meanReturn = withVolatility.select(mean("daily_return")).first().getDouble(0)
    val stdReturn = withVolatility.select(stddev("daily_return")).first().getDouble(0)
    
    withVolatility.withColumn("is_outlier",
      abs(col("daily_return") - lit(meanReturn)) > (lit(3) * lit(stdReturn))
    )
  }

  def streamTransform(
    inputStream: DataFrame,
    checkpointLocation: String,
    outputPath: String
  ): StreamingQuery = {
    val transformedStream = inputStream.transform(transform)
    
    transformedStream.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpointLocation)
      .option("path", outputPath)
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()
  }
}

class NOAADataTransformer extends DataTransformer {
  private val metricsWindow = Duration.ofMinutes(5)
  private val metricsStore = mutable.Map[String, Double]()
  
  override def transform(df: DataFrame): DataFrame = {
    validateData(df)

    // Parse and normalize weather fields
    val withParsed = df
      // Normalize temperature to Celsius (if in F)
      .withColumn("temperature_celsius",
        when(col("temperature_unit") === "F", (col("temperature") - 32) * 5 / 9)
         .otherwise(col("temperature")))
      // Parse wind speed (e.g., "5 to 15 mph" -> 15)
      .withColumn("wind_speed_max_mph",
        regexp_extract(col("wind_speed"), "(\\d+)(?: to (\\d+))? mph", 2)
          .cast(DoubleType)
          .when(length(regexp_extract(col("wind_speed"), "(\\d+)(?: to (\\d+))? mph", 2)) > 0,
                regexp_extract(col("wind_speed"), "(\\d+)(?: to (\\d+))? mph", 2).cast(DoubleType))
          .otherwise(regexp_extract(col("wind_speed"), "(\\d+)", 1).cast(DoubleType))
      )
      .withColumn("precipitation_probability_pct", col("precipitation_probability").cast(DoubleType))
      .withColumn("short_forecast_lower", lower(col("short_forecast")))
      .withColumn("detailed_forecast_lower", lower(col("detailed_forecast")))

    // Actionable weather warning logic
    val withWarnings = withParsed
      .withColumn("weather_warning",
        when(col("wind_speed_max_mph") >= 30, "Wind Advisory")
         .when(col("precipitation_probability_pct") >= 70, "Heavy Rain Warning")
         .when(col("temperature_celsius") >= 35, "Heat Warning")
         .when(col("temperature_celsius") <= 0, "Freeze Warning")
         .when(col("short_forecast_lower").contains("flood"), "Flood Watch")
         .when(col("short_forecast_lower").contains("snow"), "Snow Warning")
         .otherwise("None")
      )
      .withColumn("warning_level",
        when(col("weather_warning") === "None", "Normal")
         .when(col("weather_warning").isin("Wind Advisory", "Flood Watch", "Snow Warning"), "Advisory")
         .when(col("weather_warning").isin("Heavy Rain Warning", "Heat Warning", "Freeze Warning"), "Warning")
         .otherwise("Unknown")
      )
      .withColumn("warning_message",
        when(col("weather_warning") === "Wind Advisory", concat(lit("High winds expected: "), col("wind_speed")))
         .when(col("weather_warning") === "Heavy Rain Warning", lit("Heavy rain likely. Take precautions."))
         .when(col("weather_warning") === "Heat Warning", lit("Extreme heat. Stay hydrated and avoid outdoor activities."))
         .when(col("weather_warning") === "Freeze Warning", lit("Freezing temperatures expected. Protect plants and pipes."))
         .when(col("weather_warning") === "Flood Watch", lit("Flooding possible. Stay alert for updates."))
         .when(col("weather_warning") === "Snow Warning", lit("Snow expected. Prepare for winter conditions."))
         .otherwise(lit("No significant weather warnings."))
      )

    // Add time, location, and monitoring features (as before)
    val withLocation = withWarnings
      .withColumn("processing_timestamp", current_timestamp())
      .withColumn("year", year(col("processing_timestamp")))
      .withColumn("month", month(col("processing_timestamp")))
      .withColumn("day", dayofmonth(col("processing_timestamp")))
      .withColumn("hour", hour(col("processing_timestamp")))
      .withColumn("day_of_week", dayofweek(col("processing_timestamp")))
      .withColumn("is_weekend", when(col("day_of_week").isin(1, 7), true).otherwise(false))
      .withColumn("season",
        when(month(col("processing_timestamp")).between(3, 5), "Spring")
          .when(month(col("processing_timestamp")).between(6, 8), "Summer")
          .when(month(col("processing_timestamp")).between(9, 11), "Fall")
          .otherwise("Winter")
      )
      .withColumn("is_daylight_savings", when(month(col("processing_timestamp")).between(3, 11), true).otherwise(false))
      .withColumn("is_coastal", when(col("city").isin("Ketchikan", "Juneau", "Sitka"), true).otherwise(false))
      .withColumn("weather_pattern",
        when(col("season") === "Summer" && col("is_coastal"), "Coastal Summer")
          .when(col("season") === "Winter" && col("is_coastal"), "Coastal Winter")
          .when(col("season") === "Summer", "Inland Summer")
          .when(col("season") === "Winter", "Inland Winter")
          .otherwise("Transitional"))

    // Data quality and monitoring (as before)
    val withQuality = withLocation
      .withColumn("data_completeness_score",
        when(col("temperature").isNotNull && col("wind_speed").isNotNull && col("precipitation_probability").isNotNull, 1.0)
          .otherwise(0.5))
      .withColumn("overall_quality_score", col("data_completeness_score"))
      .withColumn("data_freshness_minutes", unix_timestamp(current_timestamp()) - unix_timestamp(col("processing_timestamp")))
      .withColumn("is_data_stale", when(col("data_freshness_minutes") > 30, true).otherwise(false))
      .withColumn("needs_attention", when(col("is_data_stale") || col("overall_quality_score") < 0.7, true).otherwise(false))

    withQuality
  }

  def streamTransform(
    inputStream: DataFrame,
    checkpointLocation: String,
    outputPath: String,
    triggerInterval: String = "1 minute"
  ): StreamingQuery = {
    // Add streaming query listener for monitoring
    val listener = new StreamingQueryListener {
      override def onQueryStarted(event: QueryStartedEvent): Unit = {
        println(s"Query started: ${event.id}")
      }
      
      override def onQueryProgress(event: QueryProgressEvent): Unit = {
        val progress = event.progress
        println(s"Processed ${progress.numInputRows} rows in ${progress.durationMs}ms")
        println(s"Input rate: ${progress.inputRowsPerSecond} rows/second")
        println(s"Processing rate: ${progress.processedRowsPerSecond} rows/second")
      }
      
      override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
        println(s"Query terminated: ${event.id}")
      }
    }
    
    inputStream.sparkSession.streams.addListener(listener)
    
    // Create the streaming query with enhanced monitoring
    val query = createStreamingQuery(
      inputStream.transform(transform),
      checkpointLocation,
      outputPath,
      triggerInterval
    )
    
    query.awaitTermination()
    query
  }
  
  def createStreamingAggregations(
    inputStream: DataFrame,
    checkpointLocation: String,
    outputPath: String
  ): StreamingQuery = {
    // Create sliding window aggregations with enhanced metrics
    val aggregatedStream = inputStream
      .withWatermark("processing_timestamp", "10 minutes")
      .groupBy(
        window(col("processing_timestamp"), "5 minutes", "1 minute"),
        col("grid_id"),
        col("city"),
        col("season"),
        col("weather_pattern")
      )
      .agg(
        count("*").as("observation_count"),
        avg("overall_quality_score").as("avg_quality_score"),
        avg("data_freshness_minutes").as("avg_data_freshness"),
        count(when(col("is_data_stale"), true)).as("stale_data_count"),
        count(when(col("needs_attention"), true)).as("attention_needed_count"),
        collect_set("forecast_office").as("forecast_offices"),
        collect_set("radar_station").as("radar_stations")
      )
    
    // Write aggregated results with partitioning
    aggregatedStream.writeStream
      .format("parquet")
      .outputMode(OutputMode.Append)
      .option("checkpointLocation", s"$checkpointLocation/aggregations")
      .option("path", s"$outputPath/aggregations")
      .partitionBy("year", "month", "day", "grid_id")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()
  }

  def streamTransformWithS3Upload(
    inputStream: DataFrame,
    checkpointLocation: String,
    localOutputPath: String,
    s3OutputPath: String,
    triggerInterval: String = "1 minute"
  ): StreamingQuery = {
    // Add streaming query listener for monitoring
    val listener = new StreamingQueryListener {
      override def onQueryStarted(event: QueryStartedEvent): Unit = {
        println(s"Query started: ${event.id}")
      }
      override def onQueryProgress(event: QueryProgressEvent): Unit = {
        val progress = event.progress
        println(s"Processed ${progress.numInputRows} rows in ${progress.durationMs}ms")
        println(s"Input rate: ${progress.inputRowsPerSecond} rows/second")
        println(s"Processing rate: ${progress.processedRowsPerSecond} rows/second")
      }
      override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
        println(s"Query terminated: ${event.id}")
      }
    }
    inputStream.sparkSession.streams.addListener(listener)

    // Use foreachBatch to write locally and upload to S3
    val query = inputStream.transform(transform).writeStream
      .outputMode(OutputMode.Append)
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(triggerInterval))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val batchPath = s"$localOutputPath/batch_$batchId"
        batchDF.write.mode("overwrite").parquet(batchPath)
        // Upload to S3 using AWS CLI (ensure AWS CLI is installed and configured)
        val s3BatchPath = s"$s3OutputPath/batch_$batchId"
        val cmd = s"aws s3 cp --recursive $batchPath $s3BatchPath"
        import sys.process._
        val exitCode = cmd.!
        if (exitCode != 0) {
          throw new RuntimeException(s"Failed to upload batch $batchId to S3")
        }
      }
      .start()
    query.awaitTermination()
    query
  }
}

// Separate object for the main method
object DataTransformerApp {
  def apply(source: String)(implicit spark: SparkSession): DataTransformer = {
    source.toLowerCase match {
      case "eosdis" => new EOSDISTransformer
      case "alphavantage" => new FinancialDataTransformer
      case "noaa" => new NOAADataTransformer
      case _ => throw new IllegalArgumentException(s"Unknown source: $source")
    }
  }

  def main(args: Array[String]): Unit = {
    // Create Spark session with S3 support
    val spark = SparkSession.builder()
      .appName("Data Transformation Job")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
      .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
      .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
      .config("spark.hadoop.fs.s3a.connection.maximum", "100")
      .getOrCreate()

    //debugging
    spark.conf.getAll.foreach { case (k, v) => println(s"$k = $v") }

    if (args.length < 3) {
      println("Usage: DataTransformerApp <input_path> <source_type> <output_path> [local_tmp_path] [s3_output_path]")
      println("source_type options: eosdis, alphavantage, noaa")
      System.exit(1)
    }

    val inputPath = args(0)
    val sourceType = args(1)
    val outputPath = args(2)
    val localTmpPath = if (args.length > 3) args(3) else "/tmp/spark_batches"
    val s3OutputPath = if (args.length > 4) args(4) else outputPath

    try {
      // Read data from input path
      val inputDF = spark.read
        .option("multiline", "true")
        .json(inputPath)

      println(s"Read ${inputDF.count()} records from $inputPath")

      // Get appropriate transformer
      val transformer = DataTransformerApp(sourceType)(spark)

      // If streaming, use foreachBatch for S3 upload
      if (sourceType.toLowerCase == "noaa") {
        println("Starting streaming transform with S3 upload via foreachBatch...")
        val streamingQuery = transformer.asInstanceOf[NOAADataTransformer]
          .streamTransformWithS3Upload(inputDF, s"$localTmpPath/checkpoint", localTmpPath, s3OutputPath)
        streamingQuery.awaitTermination()
      } else {
        // Batch mode as before
        val transformedDF = transformer.transform(inputDF)
        println(s"Transformed data has ${transformedDF.count()} records")
        transformedDF.write
          .mode("overwrite")
          .partitionBy("year", "month", "day")
          .parquet(outputPath)
        println(s"Successfully wrote transformed data to $outputPath")
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
} 