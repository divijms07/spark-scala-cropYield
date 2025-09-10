package etl

import org.apache.spark.sql.SparkSession
object SparkSessionHelper {
  def getSpark(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]") // Use all available cores
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
  }
}
