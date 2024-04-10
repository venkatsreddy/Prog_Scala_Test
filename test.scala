import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._

object BatchAggregationJob {
  def main(args: Array[String]): Unit = {
    // Configuration
    val inputPath = args(0) // Input path from command line argument
    val outputPath = args(1) // Output path from command line argument

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("BatchAggregationJob")
      .getOrCreate()

    // Import implicits for converting RDDs and DataFrames to DataSets
    import spark.implicits._

    // Read data from the input CSV file
    val inputData = spark.read
      .option("header", "true")
      .csv(inputPath)

    // Convert timestamp column to timestamp type
    val dfWithTimestamp = inputData.withColumn("timestamp", $"timestamp".cast("timestamp"))

    // Extract time bucket from timestamp (e.g., hourly bucket)
    val dfWithTimeBucket = dfWithTimestamp
      .withColumn("time_bucket", date_format($"timestamp", "yyyy-MM-dd HH:00:00"))

    // Perform aggregation
    val aggregatedData = dfWithTimeBucket
      .groupBy($"metric", $"time_bucket")
      .agg(
        avg($"value").as("average_value"),
        min($"value").as("min_value"),
        max($"value").as("max_value")
      )

    // Write aggregated data to output CSV file
    aggregatedData.write
      .mode(SaveMode.Overwrite)
      .csv(outputPath)

    // Stop the SparkSession
    spark.stop()
  }
}
