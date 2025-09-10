package utils

import com.typesafe.config.ConfigFactory

object DBUtils {
  private val config = ConfigFactory.load()

  val url: String = config.getString("db.url")
  val user: String = config.getString("db.user")
  val password: String = config.getString("db.password")
  val table: String = config.getString("db.table")
}