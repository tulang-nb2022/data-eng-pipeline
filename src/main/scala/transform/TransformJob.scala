package transform

import org.apache.spark.sql.SparkSession

object TransformJob {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Data Transformation")
      .config("spark.driver.memory", "4g")
      .getOrCreate()
    
    try {
      // Read input data
      val inputPath = args(0)
      val source = args(1)
      val outputPath = args(2)
      
      val inputDf = spark.read.json(inputPath)
      
      // Get appropriate transformer and transform data
      val transformer = DataTransformer(source)
      val transformedDf = transformer.transform(inputDf)
      
      // Write output
      transformedDf.write
        .mode("overwrite")
        .parquet(outputPath)
        
    } finally {
      spark.stop()
    }
  }
} 