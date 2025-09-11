package batch

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import com.typesafe.config.{Config, ConfigFactory}
import scala.util.{Try, Success, Failure}

/**
 * Standalone Spark Batch Job for Silver Layer Processing
 * 
 * This job processes bronze layer data and creates cleaned, enriched silver layer data
 * with comprehensive data quality rules and business logic.
 */
object SilverLayerBatchJob {
  
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: SilverLayerBatchJob <bronze_s3_path> <silver_s3_path> [processing_date]")
      println("Example: SilverLayerBatchJob s3://my-bucket/bronze/weather s3://my-bucket/silver/weather 2024-01-15")
      System.exit(1)
    }
    
    val bronzeS3Path = args(0)
    val silverS3Path = args(1)
    val processingDate = if (args.length > 2) args(2) else getCurrentDate()
    
    // Create Spark session with Delta Lake configuration
    val spark = SparkSession.builder()
      .appName("Silver Layer Batch Job - Data Quality & Enrichment")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.adaptive.skewJoin.enabled", "true")
      .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try {
      println("="*80)
      println("SILVER LAYER BATCH JOB - DATA QUALITY & ENRICHMENT")
      println("="*80)
      println(s"Bronze S3 Path: $bronzeS3Path")
      println(s"Silver S3 Path: $silverS3Path")
      println(s"Processing Date: $processingDate")
      println("="*80)
      
      // Run the silver layer processing
      val result = processSilverLayer(spark, bronzeS3Path, silverS3Path, processingDate)
      
      if (result.success) {
        println("✅ Silver layer processing completed successfully!")
        println(s"📊 Processed ${result.recordCount} records")
        println(s"📈 Data quality score: ${result.avgQualityScore}")
        println(s"💾 Data saved to: $silverS3Path")
      } else {
        println("❌ Silver layer processing failed!")
        System.exit(1)
      }
      
    } catch {
      case e: Exception =>
        println(s"❌ Error during silver layer processing: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
    }
  }
  
  case class ProcessingResult(
    success: Boolean,
    recordCount: Long,
    validRecords: Long,
    invalidRecords: Long,
    avgQualityScore: Double,
    processingTimeMs: Long
  )
  
  def processSilverLayer(
    spark: SparkSession, 
    bronzeS3Path: String, 
    silverS3Path: String, 
    processingDate: String
  ): ProcessingResult = {
    
    val startTime = System.currentTimeMillis()
    
    try {
      // Step 1: Read bronze data
      println("📖 Reading bronze data...")
      val bronzeData = spark.read
        .format("delta")
        .load(bronzeS3Path)
        .filter(col("year") === year(lit(processingDate)) && 
                col("month") === month(lit(processingDate)) && 
                col("day") === dayofmonth(lit(processingDate)))
      
      val bronzeCount = bronzeData.count()
      println(s"📊 Found $bronzeCount records in bronze layer for $processingDate")
      
      if (bronzeCount == 0) {
        println("⚠️  No data found for the specified date")
        return ProcessingResult(false, 0, 0, 0, 0.0, 0)
      }
      
      // Step 2: Apply silver layer transformations
      println("🔧 Applying data quality transformations...")
      val silverData = applySilverTransformations(bronzeData)
      
      // Step 3: Calculate data quality metrics
      println("📈 Calculating data quality metrics...")
      val qualityMetrics = silverData
        .agg(
          count("*").as("total_records"),
          count(when(col("is_valid") === true, 1)).as("valid_records"),
          count(when(col("is_valid") === false, 1)).as("invalid_records"),
          avg("quality_score").as("avg_quality_score"),
          min("processing_timestamp").as("earliest_record"),
          max("processing_timestamp").as("latest_record")
        )
        .collect()(0)
      
      val totalRecords = qualityMetrics.getAs[Long]("total_records")
      val validRecords = qualityMetrics.getAs[Long]("valid_records")
      val invalidRecords = qualityMetrics.getAs[Long]("invalid_records")
      val avgQualityScore = qualityMetrics.getAs[Double]("avg_quality_score")
      
      println("📊 Data Quality Metrics:")
      println(s"   Total Records: $totalRecords")
      println(s"   Valid Records: $validRecords")
      println(s"   Invalid Records: $invalidRecords")
      println(s"   Average Quality Score: ${avgQualityScore.formatted("%.3f")}")
      println(s"   Data Quality Rate: ${(validRecords.toDouble / totalRecords * 100).formatted("%.1f")}%")
      
      // Step 4: Write silver data with optimizations
      println("💾 Writing silver data...")
      silverData.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "true")
        .partitionBy("year", "month", "day")
        .save(silverS3Path)
      
      // Step 5: Optimize the table
      println("⚡ Optimizing table for better performance...")
      spark.sql(s"OPTIMIZE delta.`$silverS3Path`")
      
      val processingTime = System.currentTimeMillis() - startTime
      
      ProcessingResult(
        success = true,
        recordCount = totalRecords,
        validRecords = validRecords,
        invalidRecords = invalidRecords,
        avgQualityScore = avgQualityScore,
        processingTimeMs = processingTime
      )
      
    } catch {
      case e: Exception =>
        println(s"❌ Error processing silver layer: ${e.getMessage}")
        e.printStackTrace()
        ProcessingResult(false, 0, 0, 0, 0.0, System.currentTimeMillis() - startTime)
    }
  }
  
  def applySilverTransformations(df: DataFrame): DataFrame = {
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
        .otherwise(to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
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
  
  def getCurrentDate(): String = {
    import java.time.LocalDate
    import java.time.format.DateTimeFormatter
    LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  }
}