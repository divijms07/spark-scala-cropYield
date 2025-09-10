package etl

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.NumericType
import org.apache.spark.sql.expressions.Window

object TransformerSilver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionHelper.getSpark("CropYield-Silver")
    spark.sparkContext.setLogLevel("ERROR")

    val bronzePath = "src/main/resources/data/bronze/crop_yield_v8_parquet"
    val df = spark.read.parquet(bronzePath)

    println("✅ Bronze Data Loaded (from Parquet)")
    df.printSchema()
    df.show(10, truncate = false)

    // 1. Count Missing Values
    df.select(df.columns.map(c => sum(when(col(c).isNull, 1)).alias(c)): _*).show()

    // 2. Handle Missing Values
    val numericCols = df.schema.fields.collect {
      case f if f.dataType.isInstanceOf[NumericType] => f.name
    }

    var cleanedDF = df
    numericCols.foreach { colName =>
      try {
        val meanVal = df.select(mean(col(colName))).first().getDouble(0)
        cleanedDF = cleanedDF.na.fill(Map(colName -> meanVal))
      } catch {
        case e: Exception => println(s"⚠️ Skipping column '$colName' due to error: ${e.getMessage}")
      }
    }

    // 3. Normalize & Round Units
    if (cleanedDF.columns.contains("Rainfall_mm")) {
      cleanedDF = cleanedDF
        .withColumn("Rainfall_mm", round(col("Rainfall_mm"), 2))
        .withColumn("Rainfall_cm", round(col("Rainfall_mm") / 10.0, 2))
    }

    if (cleanedDF.columns.contains("Temperature_Celsius")) {
      cleanedDF = cleanedDF
        .withColumn("Temperature_Celsius", round(col("Temperature_Celsius"), 2))
        .withColumnRenamed("Temperature_Celsius", "Temperature_C")
    }

    // 4. Cast Boolean to Integer
    Seq("Fertilizer_Used", "Irrigation_Used").foreach { colName =>
      if (cleanedDF.columns.contains(colName)) {
        cleanedDF = cleanedDF.withColumn(colName, col(colName).cast("boolean").cast("int"))
      }
    }

    // 5. Feature Engineering
    if (cleanedDF.columns.contains("Rainfall_cm") && cleanedDF.columns.contains("Region")) {
      val windowSpec = Window.partitionBy("Region")
      cleanedDF = cleanedDF
        .withColumn("Avg_Rainfall", round(avg(col("Rainfall_cm")).over(windowSpec), 2))
        .withColumn("Rainfall_Deviation", round(col("Rainfall_cm") - col("Avg_Rainfall"), 2))
    }

    if (Set("N", "P", "K").subsetOf(cleanedDF.columns.toSet)) {
      cleanedDF = cleanedDF.withColumn(
        "Soil_Fertility_Index",
        round((col("N") + col("P") + col("K")) / 3, 2)
      )
    }

    cleanedDF = cleanedDF
      .withColumn("N_to_P", round(col("N") / when(col("P") =!= 0, col("P")), 2))
      .withColumn("P_to_K", round(col("P") / when(col("K") =!= 0, col("K")), 2))
      .withColumn("N_to_K", round(col("N") / when(col("K") =!= 0, col("K")), 2))

    if (cleanedDF.columns.contains("Fertilizer_Used") && cleanedDF.columns.contains("Irrigation_Used")) {
      cleanedDF = cleanedDF.withColumn("Support_Level", col("Fertilizer_Used") + col("Irrigation_Used"))
    }

    if (cleanedDF.columns.contains("Weather_Condition")) {
      Seq("Sunny", "Rainy", "Cloudy").foreach { condition =>
        cleanedDF = cleanedDF.withColumn(s"Weather_$condition", when(col("Weather_Condition") === condition, 1).otherwise(0))
      }
    }

    if (Set("Rainfall_Deviation", "Temperature_C", "Days_to_Harvest").subsetOf(cleanedDF.columns.toSet)) {
      cleanedDF = cleanedDF.withColumn(
        "Climate_Stress_Index",
        round((col("Rainfall_Deviation") * col("Temperature_C")) / col("Days_to_Harvest"), 2)
      )
    }

    cleanedDF.show(5, truncate = false)

    // 6. Save Silver Layer
    val silverParquetPath = "src/main/resources/data/silver/crop_yield_v8_parquet"
    val silverCsvPath = "src/main/resources/data/silver/crop_yield_v8_csv"

    cleanedDF.write.mode("overwrite").partitionBy("Crop", "Region").parquet(silverParquetPath)
    cleanedDF.write.mode("overwrite").option("header", "true").csv(silverCsvPath)

    println(s"✅ Silver data saved at: $silverParquetPath (Parquet) and $silverCsvPath (CSV)")
    spark.stop()
  }
}
