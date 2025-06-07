package transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.{Trigger, OutputMode, StreamingQuery, StreamingQueryListener}
import io.delta.tables._
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryStartedEvent, QueryProgressEvent, QueryTerminatedEvent}
import java.time.{Duration, Instant}
import scala.collection.mutable
import com.typesafe.config.{Config, ConfigFactory}

trait DataTransformer {
  protected val config: Config = ConfigFactory.load("weather_config.yml")
  
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
      val col = accDf(colName)
      if (col.dataType == StringType) {
        accDf.withColumn(colName,
          regexp_replace(regexp_replace(col, "\\$", ""), ",", "").cast(DoubleType)
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
    
    // Extract location metadata
    val withLocation = df
      .withColumn("forecast_office", get_json_object(col("properties"), "$.forecastOffice"))
      .withColumn("grid_id", get_json_object(col("properties"), "$.gridId"))
      .withColumn("grid_x", get_json_object(col("properties"), "$.gridX").cast(IntegerType))
      .withColumn("grid_y", get_json_object(col("properties"), "$.gridY").cast(IntegerType))
      .withColumn("timezone", get_json_object(col("properties"), "$.timeZone"))
      .withColumn("radar_station", get_json_object(col("properties"), "$.radarStation"))
      .withColumn("relative_location", get_json_object(col("properties"), "$.relativeLocation"))
      .withColumn("city", get_json_object(col("relative_location"), "$.properties.city"))
      .withColumn("state", get_json_object(col("relative_location"), "$.properties.state"))
      .withColumn("distance_meters", get_json_object(col("relative_location"), "$.properties.distance.value").cast(DoubleType))
      .withColumn("bearing_degrees", get_json_object(col("relative_location"), "$.properties.bearing.value").cast(DoubleType))
    
    // Add forecast URLs for further data enrichment
    val withUrls = withLocation
      .withColumn("forecast_url", get_json_object(col("properties"), "$.forecast"))
      .withColumn("forecast_hourly_url", get_json_object(col("properties"), "$.forecastHourly"))
      .withColumn("forecast_grid_data_url", get_json_object(col("properties"), "$.forecastGridData"))
      .withColumn("observation_stations_url", get_json_object(col("properties"), "$.observationStations"))
    
    // Add time-based features
    val withTimeFeatures = withUrls
      .withColumn("processing_timestamp", current_timestamp())
      .withColumn("year", year(col("processing_timestamp")))
      .withColumn("month", month(col("processing_timestamp")))
      .withColumn("day", dayofmonth(col("processing_timestamp")))
      .withColumn("hour", hour(col("processing_timestamp")))
      .withColumn("day_of_week", dayofweek(col("processing_timestamp")))
      .withColumn("is_weekend", 
        when(col("day_of_week").isin(1, 7), true).otherwise(false))
    
    // Add seasonal features
    val withSeasonal = withTimeFeatures
      .withColumn("season",
        when(month(col("processing_timestamp")).between(3, 5), "Spring")
          .when(month(col("processing_timestamp")).between(6, 8), "Summer")
          .when(month(col("processing_timestamp")).between(9, 11), "Fall")
          .otherwise("Winter")
      )
      .withColumn("is_daylight_savings", 
        when(month(col("processing_timestamp")).between(3, 11), true).otherwise(false))
    
    // Add location-based features
    val withLocationFeatures = withSeasonal
      .withColumn("is_coastal", 
        when(col("city").isin("Ketchikan", "Juneau", "Sitka"), true).otherwise(false))
      .withColumn("elevation_zone",
        when(col("distance_meters") < 1000, "Low")
          .when(col("distance_meters") < 2000, "Medium")
          .otherwise("High"))
    
    // Add weather pattern detection
    val withPatterns = withLocationFeatures
      .withColumn("weather_pattern",
        when(col("season") === "Summer" && col("is_coastal"), "Coastal Summer")
          .when(col("season") === "Winter" && col("is_coastal"), "Coastal Winter")
          .when(col("season") === "Summer", "Inland Summer")
          .when(col("season") === "Winter", "Inland Winter")
          .otherwise("Transitional"))
    
    // Add data quality metrics
    val withQuality = withPatterns
      .withColumn("data_completeness_score",
        when(col("forecast_url").isNotNull && 
             col("forecast_hourly_url").isNotNull && 
             col("forecast_grid_data_url").isNotNull, 1.0)
          .otherwise(0.5))
      .withColumn("location_accuracy_score",
        when(col("distance_meters") < 1000, 1.0)
          .when(col("distance_meters") < 2000, 0.8)
          .otherwise(0.6))
      .withColumn("overall_quality_score",
        (col("data_completeness_score") + col("location_accuracy_score")) / 2)
    
    // Add real-time monitoring features
    val withMonitoring = withQuality
      .withColumn("data_freshness_minutes",
        unix_timestamp(current_timestamp()) - unix_timestamp(col("processing_timestamp")))
      .withColumn("is_data_stale",
        when(col("data_freshness_minutes") > 30, true).otherwise(false))
      .withColumn("needs_attention",
        when(col("is_data_stale") || col("overall_quality_score") < 0.7, true)
          .otherwise(false))
    
    withMonitoring
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
}

object DataTransformer {
  def apply(source: String)(implicit spark: SparkSession): DataTransformer = {
    source.toLowerCase match {
      case "eosdis" => new EOSDISTransformer
      case "alphavantage" => new FinancialDataTransformer
      case "noaa" => new NOAADataTransformer
      case _ => throw new IllegalArgumentException(s"Unknown source: $source")
    }
  }
} 