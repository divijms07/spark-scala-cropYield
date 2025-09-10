package etl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object GoldEnricher {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionHelper.getSpark("CropYield-Gold")
    spark.sparkContext.setLogLevel("ERROR")

    val silverPath = "src/main/resources/data/silver/crop_yield_v8_parquet"
    val df = spark.read.parquet(silverPath)

    println("✅ Silver Data Loaded (from Parquet)")
    df.printSchema()
    df.show(5, truncate = false)

    val colsToKeep = Seq(
      "Region", "Crop", "Soil_Type", "Days_to_Harvest",
      "Rainfall_cm", "Temperature_C", "Yield_tons_per_hectare",
      "Rainfall_Deviation", "Soil_Fertility_Index",
      "N_to_P", "P_to_K", "N_to_K",
      "Support_Level",
      "Weather_Sunny", "Weather_Rainy", "Weather_Cloudy",
      "Climate_Stress_Index"
    )

    val goldDF = df.select(colsToKeep.filter(df.columns.contains).map(col): _*)

    println("✅ Gold Dataset Ready (Curated Features)")
    goldDF.show(10, truncate = false)

    val goldParquetPath = "src/main/resources/data/gold/crop_yield_v8_parquet"
    val goldCsvPath = "src/main/resources/data/gold/crop_yield_v8_csv"

    goldDF.repartition(1).write.mode("overwrite").parquet(goldParquetPath)
    goldDF.repartition(1).write.mode("overwrite").option("header", "true").csv(goldCsvPath)

    println(s"✅ Gold data saved at: $goldParquetPath (Parquet) and $goldCsvPath (CSV)")
    spark.stop()
  }
}
