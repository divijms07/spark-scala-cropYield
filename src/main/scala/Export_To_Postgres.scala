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
      .csv("C:\\Users\\dimahend\\Downloads\\intellij Projects\\spark-scala-cropYield\\src\\main\\resources\\data\\gold\\crop_yield_v8_csv\\goldfinal.csv")

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