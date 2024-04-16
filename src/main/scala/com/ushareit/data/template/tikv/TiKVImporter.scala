package com.ushareit.data.template.tikv

import com.ushareit.data.template.utils.SharedisUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.tikv.bulkload.RawKVBulkLoader

object TiKVImporter extends Logging{

  var tableName = ""
  var input = ""
  var segment = ""
  var pdaddr = ""
  var kvType = ""

  def main(args: Array[String]): Unit = {

    args.sliding(2, 2).toList.collect {
      case Array("--table", argTableName: String) => tableName = argTableName
      case Array("--input", argInput: String) => input = argInput
      case Array("--segment", argSegment: String) => segment = argSegment
      case Array("--pdaddr", argPdaddr: String) => pdaddr = argPdaddr
      case Array("--kvType", argKvType: String) => kvType = argKvType
    }

    assert(StringUtils.isNotEmpty(segment), "Segment can not be empty")

    val sparkConf = new SparkConf()
    val spark = SparkSession.builder()
      .appName("TiKVImporter")
      .enableHiveSupport()
      .config(sparkConf)
      .getOrCreate()

    val df = if (StringUtils.isNotEmpty(tableName)) {
      spark.read.table(tableName)
    } else if (StringUtils.isNoneEmpty(input)) {
      spark.read.parquet(input)
    } else {
      throw new Exception("table or input must be set")
    }
    val rdd = df.withColumn("segment", lit(segment)).rdd

    val sharedisKV: RDD[(Array[Byte], Array[Byte])] = if (kvType.toLowerCase == "binary") {
      rdd.map(row => {
        val key = new String(row.getAs[Array[Byte]]("key"))
        val value = row.getAs[Array[Byte]]("value")
        val member = new String(row.getAs[Array[Byte]]("member"))
        val storeType = row.getAs[Byte]("storeType")
        val segment = row.getAs[String]("segment")
        val keyBuffer = SharedisUtil.encodeKey(segment, storeType, key, member)
        (keyBuffer.array(), value)
      })
    } else {
      rdd.map(row => {
        val keyBuffer = SharedisUtil.encodeKey(row)
        val value = row.getAs("value").asInstanceOf[String]
        (keyBuffer.array(), value.getBytes())
      })
    }
    try {
      new RawKVBulkLoader(pdaddr, sparkConf).bulkLoad(sharedisKV)
    } catch {
      case e: Exception =>
        logError("Raw kv bulk loader failed ", e)
        spark.stop()
        System.exit(-1)
    }
    spark.stop()
  }



}