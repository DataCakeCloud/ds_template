package com.ushareit.data.template.migration

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object SparkSqlMigration extends Logging{

  var sourceRegion: String = _
  var targetRegion: String = _
  var partitions: String = _
  var table: String = _
  var isAcrossCloud = false
  var location: String = _

  def parseArgs(args: Array[String]): Unit = {
    if (args.length < 4) {
      logError("Usage: [--table_name string] " +
        "[--output string] [--partition_num num] ")
      sys.exit(1)
    }
    args.sliding(2, 2).toList.collect {
      case Array("-a", argAcrossCloud: String) => isAcrossCloud = argAcrossCloud.toBoolean
      case Array("-sourceRegion", argSourceRegion: String) => sourceRegion = argSourceRegion
      case Array("-targetRegion", argTargetRegion: String) => targetRegion = argTargetRegion
      case Array("-partitions", argPartitions: String) =>
        partitions = argPartitions.split(",").map(partition => {
          val partitionKV = partition.split("=")
          val value = partitionKV(1).trim
          if (!value.startsWith("'") || !value.startsWith("\"")) {
            partitionKV(1) = "'" + value + "'"
          }
          partitionKV.mkString("=")
        }).mkString(",")

      case Array("-table", argTable: String) => table = argTable
      case Array("-location", argLocation: String) => location = argLocation
    }
    assert(table != null, "should set -table")
    assert(sourceRegion != null, "should set -sourceRegion")
    assert(targetRegion != null, "should set -targetRegion")
  }

  def getParameters(sparkConf: SparkConf, region: String): String = {
    var table_type = "hive"
    try {
      val hmsUri = sparkConf.get("spark.sql.catalog.%s.uri".format(region))
      logInfo("addMetastoreConf hmsUri: " + hmsUri)
      val hiveConf = new HiveConf()
      hiveConf.set("hive.metastore.uris", hmsUri)
      val client = new HiveMetaStoreClient(hiveConf)
      val tables = table.split("\\.")
      val tableTable = client.getTable(tables(0), tables(1))
      val parameters = tableTable.getParameters
      logInfo("------getParameters params: " + parameters.toString)
      table_type = if (parameters.get("table_type")!=null) parameters.get("table_type") else "hive"
    } catch {
      case e:Exception=> {
        logInfo("------getParameters params exception: "+e.printStackTrace())
      }
    }
    table_type
  }

  def addMetastoreConf(sparkConf: SparkConf): SparkConf = {
    val region = sourceRegion

    val table_type = getParameters(sparkConf, region)
    logInfo("addMetastoreConf--region:"+region)
    if (table_type.equals("hive")) {
      if (!"sg2".equals(region)) {
        sparkConf.set("spark.hadoop.hive.metastore.uris", "thrift://hms-%s.ushareit.org:9083".format(region))
      }
    } else {
      val region = targetRegion
      val table_type = getParameters(sparkConf, region)
      if (table_type.equals("hive")) {
        if (!"sg2".equals(region)) {
          sparkConf.set("spark.hadoop.hive.metastore.uris", "thrift://hms-%s.ushareit.org:9083".format(region))
        }
      }
    }

    sparkConf
  }

  def main(args: Array[String]): Unit = {
    logInfo(s"args: ${args.toBuffer}")
    parseArgs(args)
    var sparkConf = buildSparkConf()
    logInfo("sparkConf:" + sparkConf.toString)
    sparkConf = addMetastoreConf(sparkConf)

    logInfo("uri: "+sparkConf.get("spark.hadoop.hive.metastore.uris"))

    val spark = SparkSession.builder()
      .appName("spark-sql-migration")
      .enableHiveSupport()
      .config(sparkConf)
      .getOrCreate()
    val sourceTable = sourceRegion + "." + table
    val targetQualifiedName = targetRegion + "." + table
    val dataFrame = spark.read.table(sourceTable)
    val schema = dataFrame.schema
    createTable(spark, schema, targetQualifiedName)
    val whereClause = if (partitions !=null && partitions.nonEmpty) {
      "WHERE " + partitions.replaceAll(",", " AND ")
    } else {
      ""
    }
    val partitionClause = if (partitions !=null && partitions.nonEmpty) {
      s"PARTITION ($partitions) "
    } else {
      ""
    }
    val selected = schema.filter(filed => {
      !partitions.contains(filed.name)
    }).map(filed => {
      filed.name
    })
    val insertSql = s"INSERT OVERWRITE TABLE  ${targetQualifiedName} $partitionClause SELECT ${selected.mkString(",")} FROM $sourceTable $whereClause"
    logInfo(s"insert sql: $insertSql")
    spark.sql(insertSql)
  }

  def createTable(spark: SparkSession, schema: StructType, targetTable: String): Unit ={
    val partitionKeys = partitions.split(",").map(_.split("=")(0)).mkString(", ")
    val ddl = s"CREATE TABLE IF NOT EXISTS $targetTable (${schema.toDDL}) USING iceberg PARTITIONED BY (${partitionKeys}) LOCATION '${location}' "
    logInfo(s"ddl: $ddl")
//    spark.sql(ddl).show()
  }

  def buildSparkConf(): SparkConf = {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.dynamicAllocation.enabled", "true")
    if (isAcrossCloud) {
      if (sourceRegion == "sg2") {
        sparkConf.set("spark.kubernetes.executor.node.selector.direction", "ObsToS3")
      } else if (targetRegion == "sg2") {
        sparkConf.set("spark.kubernetes.executor.node.selector.direction", "S3ToObs")
      }
    }
    sparkConf
  }
}
