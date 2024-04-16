package com.ushareit.data.template.util

object NormalUtil {
  def extractHostAndPort(jdbcUrl: String): String = {
    val pattern = "jdbc:mysql://([^:/]+):(\\d+)/.*".r
    jdbcUrl match {
      case pattern(host, port) => s"$host:$port"
      case _ => ""
    }
  }
}
