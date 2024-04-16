package com.ushareit.data.template.iceberg

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

object Csv2IcebergTable extends Logging {

  var location: String = _
  var targetTable: String = _
  var partitions: Array[(String, String)] = _
  var primaryKey: String = _
  var fileNums: String = _
  var header: Boolean = true

  def main(args: Array[String]): Unit = {
    args.sliding(2, 2).toList.collect {
      case Array("-location", argLocation: String) => location = argLocation
      case Array("-targetTable", argTargetTable: String) => targetTable = argTargetTable
      case Array("-partitions", argPartitions: String) =>
        partitions = argPartitions.split(",").map(part => {
          val partitionKV = part.split("=")
          var partitionValue = partitionKV(1)
          if (partitionValue.startsWith("'") || partitionValue.startsWith("\"")) {
            partitionValue = partitionValue.substring(1)
          }
          if (partitionValue.endsWith("'") || partitionValue.endsWith("\"")) {
            partitionValue = partitionValue.substring(0, partitionValue.length - 1)
          }
          (partitionKV(0), partitionValue)
        })
      case Array("-primaryKey", argPrimaryKey: String) => primaryKey = argPrimaryKey
      case Array("-fileNums", argFileNums) => fileNums = argFileNums
      case Array("-header", argHeader) => header = argHeader.toBoolean
    }
    assert(location != null, "shoule set -location")
    assert(targetTable != null, "shoule set -targetTable")
    val sparkConf = new SparkConf()
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .config(sparkConf)
      .getOrCreate()
    val icebergTable = spark.table(targetTable)
    //var dataFrame = spark.read.format("csv").option("header", "true").load(location)
    var dataFrame =
    if (header) {
      spark.read.format("csv").option("header", "true").load(location)
    } else {
      val schema = icebergTable.schema
      val fields = schema.toList.filter(field => {
        var res = true
        for(partition <- partitions) {
          if (partition._1 == field.name) {
            res = false
          }
        }
        res
      })
      val realSchema = StructType(fields)
      logInfo(realSchema.mkString(" | "))
      spark.read.format("csv").schema(realSchema).load(location)
    }
    val fields = dataFrame.schema.map(filed => {
      if (filed.name == primaryKey) {
        logInfo(s"filed.name == $primaryKey, filed.name=${filed.name}")
        StructField(filed.name, filed.dataType, nullable = false)
      } else {
        logInfo(s"filed.name != $primaryKey, filed.name=${filed.name}")
        filed
      }
    })
    val realSchema = StructType(fields)
    logInfo(realSchema.mkString(" | "))
    var res = spark.sqlContext.createDataFrame(dataFrame.rdd, realSchema)
    res.printSchema()
    partitions.foreach(partition => {
      res = res.withColumn(partition._1, lit(partition._2))
    })
    if (fileNums != null && fileNums.toInt > 0) {
      res = res.repartition(fileNums.toInt)
    }
/*    logInfo("start dataFrame.show()============")
    dataFrame.show()
    logInfo("end dataFrame.show()============")*/
    res.writeTo(targetTable).overwritePartitions()
  }

}
