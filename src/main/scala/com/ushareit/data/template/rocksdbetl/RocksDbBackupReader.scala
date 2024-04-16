package com.ushareit.data.template.rocksdbetl

import com.ushareit.data.hadoop.io.rocksdb.{ShareStoreInputFormat, ShareStoreWritable}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object RocksDbBackupReader extends Logging{
  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      logError("Usage: [--table_name string] [--input string] " +
        "[--output string]")
      sys.exit(1)
    }
    var tableName: String = null
    var input: String = null
    var output: String = null
    var partition: Int = 32

    args.sliding(2, 2).toList.collect {
      case Array("--table_name", argTableName: String) => tableName = argTableName
      case Array("--input", argInput: String) => input = argInput
      case Array("--output", argOutput: String) => output = argOutput
    }
    assert(tableName != null, "should set --table_name")
    assert(input != null, "should set --input")
    assert(output != null, "should set --output")

    val spark = SparkSession.builder()
      .appName("RocksDbBackupReader")
      .enableHiveSupport()
      .config(new SparkConf())
      .getOrCreate()
    val rdd = run(spark, tableName, input)
    write(spark, rdd, output)

    spark.stop()
  }

  def run(spark: SparkSession, tableName: String, input: String): RDD[ShareStoreRecord] = {
    val sparkContext = spark.sparkContext
    sparkContext.hadoopConfiguration.set(ShareStoreInputFormat.TABLE_NAME_PROP_KEY, tableName)
    sparkContext.hadoopConfiguration.set(FileInputFormat.INPUT_DIR, input)

    sparkContext.newAPIHadoopRDD(sparkContext.hadoopConfiguration, classOf[ShareStoreInputFormat],
      classOf[NullWritable], classOf[ShareStoreWritable])
      .map(x => transform(x._2))
  }

  def write(spark: SparkSession,
            rdd: RDD[ShareStoreRecord],
            output: String): Unit = {
    import spark.implicits._
    rdd.toDF()
      .write
      .option("compression", "gzip")
      .mode(SaveMode.Overwrite)
      .parquet(output)
  }

  def transform(writable: ShareStoreWritable): ShareStoreRecord = {
    ShareStoreRecord(
      writable.getType.get(),
      writable.getKey.toString,

      writable.getValue.toString,
      writable.getTtl.get(),
      writable.getMember.toString,
      writable.getScore.get()
    )
  }
}

case class ShareStoreRecord(storeType: Byte, key: String, value: String,
                            ttl: Int, member: String, score: Long)