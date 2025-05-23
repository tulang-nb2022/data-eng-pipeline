package transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
import io.delta.tables._
import org.apache.spark.sql.streaming.StreamingQuery

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

object DataTransformer {
  def apply(source: String)(implicit spark: SparkSession): DataTransformer = {
    source.toLowerCase match {
      case "eosdis" => new EOSDISTransformer
      case "alphavantage" => new FinancialDataTransformer
      case _ => throw new IllegalArgumentException(s"Unknown source: $source")
    }
  }
} 