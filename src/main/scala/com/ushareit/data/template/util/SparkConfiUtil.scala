package com.ushareit.data.template.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, DataFrameWriter}

object SparkConfiUtil {
  private var dorisConf = List("spark.doris.request.retries",
    "spark.doris.request.connect.timeout.ms",
    "spark.doris.request.read.timeout.ms",
    "spark.doris.request.query.timeout.s",
    "spark.doris.request.tablet.size",
    "spark.doris.batch.size",
    "spark.doris.exec.mem.limit",
    "spark.doris.deserialize.arrow.async",
    "spark.doris.deserialize.queue.size",
    "spark.sink.batch.size",
    "spark.sink.max-retries",
    "spark.doris.sink.task.partition.size",
    "spark.doris.sink.task.use.repartition",
    "spark.doris.sink.batch.interval.ms"

  )

  def writeAllToDorisDF(dataFrame: DataFrameWriter[_], sparkConf: SparkConf): Unit = {
    dorisConf.foreach(parameterName => {
      writeToDorisDF(dataFrame, sparkConf, parameterName)
    })
  }

  def writeToDorisDF(dfWrite: DataFrameWriter[_], sparkConf: SparkConf, parameterName: String): Unit = {
    val parameterValue = sparkConf.get(parameterName, "")
    if (parameterValue.nonEmpty) {
      dfWrite.format("doris").option(parameterName.replace("spark.",""), parameterValue)
    }
  }

}
