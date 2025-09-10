import org.apache.spark.sql.SparkSession
import utils.DBUtils

object Export_To_Postgres {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Export_To_Postgres")
      .master("local[*]")
      .getOrCreate()

    val masterDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\Users\\dimahend\\Downloads\\intellij Projects\\CropYieldPipeline\\src\\main\\resources\\data\\gold\\crop_yield_v8_csv\\part-00000-1a8020c1-9b81-41a7-a4c0-705932c940ce-c000.csv")

    masterDF.write
      .format("jdbc")
      .option("url", DBUtils.url)
      .option("dbtable", DBUtils.table)
      .option("user", DBUtils.user)
      .option("password", DBUtils.password)
      .option("driver", "org.postgresql.Driver")
      .mode("overwrite")
      .save()

    println(s"Master Data Saved")

    spark.stop()
  }
}