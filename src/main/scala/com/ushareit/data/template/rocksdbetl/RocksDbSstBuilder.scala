package com.ushareit.data.template.rocksdbetl

import java.io.Serializable

import com.ushareit.data.hadoop.io.rocksdb.{ShareStoreKV, ShareStoreOutputFormat, Utils}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{Partitioner, SparkConf}
import org.tikv.common.util.FastByteComparisons

object RocksDbSstBuilder extends Logging {

  var tableName: String = _
  var output: String = _
  var partitionNum: Int = 32
  var partition: String = _
  var segment:String = _
  var input:String=_
  def parseArgs(args: Array[String]): Unit = {
    if (args.length < 4) {
      logError("Usage: [--table_name string] " +
        "[--output string] [--partition_num num] ")
      sys.exit(1)
    }

    args.sliding(2, 2).toList.collect {
      case Array("--table_name", argTableName: String) => tableName = argTableName
      case Array("--input", argInput: String) => input = argInput
      case Array("--output", argOutput: String) => output = argOutput
      case Array("--partition_num", argPartitionNum: String) => partitionNum = argPartitionNum.toInt
      case Array("--segment",argSegment:String) => segment = argSegment
      case Array("--partitions", argPartitions: String) =>
        val partitionKV = argPartitions.split("=")
        if (partitionKV(1).startsWith("\"") || partitionKV(1).startsWith("'")) {
          partition = argPartitions
        } else {
          partition = s"${partitionKV(0)}='${partitionKV(1)}'"
        }

    }
    assert(output != null, "should set --output")
  }
  def main(args: Array[String]): Unit = {
    parseArgs(args)
    logInfo("table_name->"+tableName+"input->"+input+" output->"+output+" partition_num->"+partitionNum+" segment->"+segment+" partitions->"+partition)
    val sparkConf = new SparkConf()
    sparkConf.set("spark.sql.parquet.mergeSchema", "true")
    val spark = SparkSession.builder()
      .appName("RocksDbSstBuilder")
      .enableHiveSupport()
      .config(sparkConf)
      .getOrCreate()
    val outputPath = new Path(output)
    val fs = FileSystem.get(outputPath.toUri, spark.sparkContext.hadoopConfiguration)
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
    }
    val sparkContext = spark.sparkContext

    sparkContext.hadoopConfiguration.set(ShareStoreOutputFormat.OUTPUT_TABLE_NAME, segment)
    var df:DataFrame = null
    if (tableName != null && !tableName.isEmpty){
      df = spark.read.table(tableName).filter(partition)
    }else{
      df = spark.read.parquet(input)
    }

    val valueDataType = df.schema.fields
      .filter(field => field.name.toLowerCase() == "value").head
      .dataType.typeName.toLowerCase()
    if (valueDataType == "string") {
      df.rdd
        .map(row => {
          val kv = new ShareStoreKV()
          try {
            Utils.encodeStoreKV(row, kv)
          } catch {
            case e: Exception =>
              logError("error data: " + row.toString())
              throw e
          }
          (new Key(kv.getKey.array), kv.getValue.array)
        }).repartitionAndSortWithinPartitions(new ShareStorePartitioner(partitionNum))
        .map(kv => (new Text(kv._1.getKey), new Text(kv._2)))
        .saveAsNewAPIHadoopFile(output,
          classOf[Text],
          classOf[Text],
          classOf[ShareStoreOutputFormat],
          sparkContext.hadoopConfiguration)
    } else {
      logInfo(s"value data type is non-string: $valueDataType")
      df.rdd
        .map(row => {
          val kv = new ShareStoreKV()
          try {
            Utils.encodeStoreKVBinary(row, kv)
          } catch {
            case e: Exception =>
              logError("error data: " + row.toString())
              throw e
          }
          (new Key(kv.getKey.array), kv.getValue.array)
        }).repartitionAndSortWithinPartitions(new ShareStorePartitioner(partitionNum))
        .map(kv => (new Text(kv._1.getKey), new Text(kv._2)))
        .saveAsNewAPIHadoopFile(output,
          classOf[Text],
          classOf[BytesWritable],
          classOf[ShareStoreOutputFormat],
          sparkContext.hadoopConfiguration)
    }

    spark.stop()
  }

  class Key (var key: Array[Byte]) extends Serializable with Ordered[Key] {
    /**
     * return key
     */
    def getKey: Array[Byte] = key

    override def compare(that: Key): Int = {
      FastByteComparisons.compareTo(key, that.getKey)
    }
  }

  class ShareStorePartitioner (var partitions: Int) extends Partitioner {
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      val keyTmp = key.asInstanceOf[Key]
      val shardId = Utils.getShardId(Utils.decodeKey(keyTmp.getKey), partitions)
      shardId
    }
  }

}
