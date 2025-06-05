package transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.{Trigger, OutputMode, StreamingQuery, StreamingQueryListener}
import io.delta.tables._
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryStartedEvent, QueryProgressEvent, QueryTerminatedEvent}
import java.time.{Duration, Instant}
import scala.collection.mutable

trait DataTransformer {
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
    checkpointLocation: String,
    outputPath: String,
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
    
    // Convert temperature from tenths of Celsius to Celsius
    val withTemperature = df.withColumn("temperature_celsius",
      col("temperature").cast(DoubleType) / 10.0
    )
    
    // Convert precipitation from tenths of mm to mm
    val withPrecipitation = withTemperature.withColumn("precipitation_mm",
      col("precipitation").cast(DoubleType) / 10.0
    )
    
    // Add derived columns with real-time calculations
    val withDerived = withPrecipitation
      .withColumn("is_rainy_day", col("precipitation_mm") > 0)
      .withColumn("temperature_fahrenheit",
        (col("temperature_celsius") * 9/5) + 32
      )
      .withColumn("processing_timestamp", current_timestamp())
      .withColumn("data_quality_score", lit(1.0))  // For Great Expectations validation
    
    // Add date-based features
    val withDateFeatures = withDerived
      .withColumn("year", year(col("date")))
      .withColumn("month", month(col("date")))
      .withColumn("day", dayofmonth(col("date")))
      .withColumn("season",
        when(month(col("date")).between(3, 5), "Spring")
          .when(month(col("date")).between(6, 8), "Summer")
          .when(month(col("date")).between(9, 11), "Fall")
          .otherwise("Winter")
      )
    
    // Add real-time weather patterns
    val withPatterns = withDateFeatures
      .withColumn("temperature_trend",
        window(col("processing_timestamp"), "5 minutes").over(
          Window.partitionBy("station_id").orderBy("processing_timestamp")
        )
      )
      .withColumn("is_heat_wave",
        when(
          col("temperature_celsius") > 30 &&
          lag("temperature_celsius", 1).over(Window.partitionBy("station_id").orderBy("processing_timestamp")) > 30 &&
          lag("temperature_celsius", 2).over(Window.partitionBy("station_id").orderBy("processing_timestamp")) > 30,
          true
        ).otherwise(false)
      )
      .withColumn("is_cold_snap",
        when(
          col("temperature_celsius") < 0 &&
          lag("temperature_celsius", 1).over(Window.partitionBy("station_id").orderBy("processing_timestamp")) < 0 &&
          lag("temperature_celsius", 2).over(Window.partitionBy("station_id").orderBy("processing_timestamp")) < 0,
          true
        ).otherwise(false)
      )
    
    // Add real-time anomaly detection
    val withAnomalies = withPatterns
      .withColumn("temperature_anomaly",
        when(
          abs(col("temperature_celsius") - 
            avg("temperature_celsius").over(
              Window.partitionBy("station_id")
                .orderBy("processing_timestamp")
                .rowsBetween(-10, 0)
            )) > 
          3 * stddev("temperature_celsius").over(
            Window.partitionBy("station_id")
              .orderBy("processing_timestamp")
              .rowsBetween(-10, 0)
          ),
          true
        ).otherwise(false)
      )
    
    withAnomalies
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
    
    // Create the streaming query
    val query = createStreamingQuery(
      inputStream.transform(transform),
      checkpointLocation,
      outputPath,
      triggerInterval
    )
    
    // Add watermark for late data handling
    query.awaitTermination()
    query
  }
  
  def createStreamingAggregations(
    inputStream: DataFrame,
    checkpointLocation: String,
    outputPath: String
  ): StreamingQuery = {
    // Create sliding window aggregations
    val aggregatedStream = inputStream
      .withWatermark("processing_timestamp", "10 minutes")
      .groupBy(
        window(col("processing_timestamp"), "5 minutes", "1 minute"),
        col("station_id")
      )
      .agg(
        avg("temperature_celsius").as("avg_temperature"),
        max("temperature_celsius").as("max_temperature"),
        min("temperature_celsius").as("min_temperature"),
        sum("precipitation_mm").as("total_precipitation"),
        count("*").as("observation_count"),
        avg("data_quality_score").as("data_quality_score")  // For monitoring
      )
    
    // Write aggregated results
    aggregatedStream.writeStream
      .format("parquet")
      .outputMode(OutputMode.Append)
      .option("checkpointLocation", s"$checkpointLocation/aggregations")
      .option("path", s"$outputPath/aggregations")
      .partitionBy("year", "month", "day")
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