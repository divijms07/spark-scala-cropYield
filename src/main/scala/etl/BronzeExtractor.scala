package etl

import org.apache.spark.sql.SparkSession
import java.nio.file.{Paths, Files}

object BronzeExtractor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionHelper.getSpark("CropYield-Bronze")
    spark.sparkContext.setLogLevel("ERROR")

    val rawPath = Paths.get("src/main/resources/data/raw/crop_yield.csv").toAbsolutePath.toString
    if (!Files.exists(Paths.get(rawPath))) {
      throw new RuntimeException(s"CSV file not found at $rawPath")
    }

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(rawPath)

    println(s"✅ Loaded Raw Data\nRows: ${df.count()} \nColumns: ${df.columns.length}")
    df.printSchema()
    df.show(10, truncate = false)

    val expectedColumns = Set("N", "P", "K", "Soil_Fertility_Index")
    val missing = expectedColumns.diff(df.columns.toSet)
    if (missing.nonEmpty) {
      throw new RuntimeException(s"Missing expected columns: $missing")
    }

    val bronzeParquetPath = "src/main/resources/data/bronze/crop_yield_v8_parquet"
    val bronzeCsvPath = "src/main/resources/data/bronze/crop_yield_v8_csv"

    df.repartition(1).write.mode("overwrite").parquet(bronzeParquetPath)
    df.repartition(1).write.mode("overwrite").option("header", "true").csv(bronzeCsvPath)

    println(s"✅ Bronze data written to: $bronzeParquetPath (Parquet) and $bronzeCsvPath (CSV)")
    spark.stop()
  }
}
