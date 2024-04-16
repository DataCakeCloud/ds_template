package com.ushareit.data.template.dbetl

import java.io.ByteArrayOutputStream
import java.sql.{DriverManager, SQLException}
import java.util
import java.util.regex.Pattern
import java.util.zip.Inflater
import java.util.{Base64, Properties}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.ushareit.data.template.util.{NormalUtil, SparkConfiUtil}
import com.ushareit.data.template.utils.EncryptUtil
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrameWriter, Row, SaveMode, SparkSession}

object DBImporter extends Logging{
  private var sourceTable: String = _
  private var targetTable: String = _
  private var dbType: String = _
  private var connectionUrl: String = _
  private var dbUser: String = _
  private var dbPassword: String = _
  private var partitions: String = _
  private var clusterName:String = _
  private var driver = "ru.yandex.clickhouse.ClickHouseDriver"

  private var truncate = false
  private var sql = ""
  private var ddl = ""
  private var fields = ""

  private def isBase64(str: String): Boolean = {
    val base64Pattern =
      "^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$"
    Pattern.matches(base64Pattern, str)
  }

  private def decode(str: String): String = {
    val dataBytes = Base64.getDecoder.decode(str.getBytes())
    val decompresser = new Inflater()
    decompresser.reset()
    decompresser.setInput(dataBytes)
    val outputStream = new ByteArrayOutputStream(dataBytes.length)
    val buf = new Array[Byte](1024)
    while (!decompresser.finished()) {
      val i = decompresser.inflate(buf)
      outputStream.write(buf, 0, i)
    }
    outputStream.close()
    decompresser.end()
    outputStream.toString()
  }

  def parseArgs(args: Array[String]): Unit = {
    logInfo(s"args: ${args.toBuffer}")
    assert(args.length >= 1, "should set a json format args")
    var json = args(0).replaceAll(" ", "")
    if (isBase64(args(0))) {
      json = decode(args(0))
    } else {
      json = args(0)
    }
    println(args.length)
    args.foreach(data=>{
      println(data)
    })
    logInfo(s"the json args: $json")
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val config = mapper.readValue(json, classOf[DBEtlConfig])
    connectionUrl = config.connectionUrl
    dbUser = config.dbUser
    dbPassword = EncryptUtil.decrypt(config.dbPassword, EncryptUtil.passwdEncryptKey)
    partitions = config.partitions
    sourceTable = config.sourceTable
    targetTable = config.targetTable
    clusterName =  config.cluster
/*    val tmp = config.sql.replaceAll(" ", "")
    if (isBase64(tmp)) {
      sql = decode(tmp)
    } else {
      sql = config.sql
    }*/
    val argSql = config.sql
    val i = argSql.length / 1024
    val count = argSql.trim.count(_==' ')
    log.info(s"i = $i")
    log.info(s"count = $count")
    val tmp = if (count <= i + 1) {
      argSql.replaceAll(" ", "")
    } else {
      argSql
    }
    log.info(s"tmp = $tmp")
    if (isBase64(tmp)) {
      log.info("sql = decode(tmp)")
      sql = decode(tmp)
    } else {
      log.info("sql = argSql")
      sql = argSql
    }
    logInfo(s"execute sql: $sql")
    dbType = config.dbType
    if (dbType == "mysql") {
      driver = "com.mysql.jdbc.Driver"
      var schema = config.columns.map(column => {
        s"${column.columnName} ${column.columnType} COMMENT '${column.comment}'"
      })
      schema ++= partitions.split(",").map(partition => {
        val partitionKV = partition.split("=")
        s"${partitionKV(0)} VARCHAR(255) COMMENT ''"
      })
      ddl = s"CREATE TABLE IF NOT EXISTS $targetTable (${schema.mkString(", ")})"
    } else if (dbType == "clickhouse") {
        logInfo("执行建表")
        driver = "ru.yandex.clickhouse.ClickHouseDriver"
        val identifier = targetTable.split("\\.")
        val dbName = identifier(0)
        val tableName = identifier(1)
        val partitionByColumns = partitions.split(",").map(partition => {
          val partitionKV = partition.split("=")
          s"${partitionKV(0)}"
        })
        var columns = config.columns.map(column => {
          s"""${column.columnName} ${column.columnType} COMMENT '${column.comment}'"""
        })
        columns ++= partitions.split(",").map(partition => {
          val partitionKV = partition.split("=")
          s"""${partitionKV(0)} String""".stripMargin
        })

        ddl =
          s"""
             |create table IF NOT EXISTS $dbName.$tableName on cluster $clusterName (
             |${columns.mkString(", ")}
             |) Engine=ReplicatedMergeTree('/clickhouse/tables/$dbName/$tableName/{shard}', '{replica}')
             | partition by (${partitionByColumns.mkString(",")})
             | order by tuple()
             |""".stripMargin


    } else if (dbType == "doris"){
      driver = "com.mysql.jdbc.Driver"
      fields = config.columns.map(_.columnName).mkString(",")
      ddl = ""

    } else {
      throw new Exception(s"不支持的db类型: $dbType")
    }

  }

  def createTable(): Unit = {
    Class.forName(driver)
    var flag = true
    for (i <- 1 to 3 if flag) {
      if (dbType == "mysql") {
        val conn = DriverManager.getConnection(connectionUrl, dbUser, dbPassword)
        val stmt = conn.createStatement()
        try {
          logInfo(s"start execute ddl:[$ddl]")
          stmt.execute(ddl)
          logInfo(s"execute ddl:[$ddl] success")
        } catch {
          case ex: SQLException =>
            logInfo(s"execute ddl:[$ddl] failed at times($i)", ex)
            throw ex
          case e: Throwable => throw e
        } finally {
          stmt.close()
          conn.close()
        }
      } else if (dbType == "clickhouse") {
        logInfo(s"-------------${clusterName}----------------")
        if (clusterName != null && !"".equals(clusterName)) {
          logInfo(s"start execute ddl:[$ddl]")
          val connection = DriverManager.getConnection(connectionUrl, dbUser, dbPassword)
          val statement = connection.createStatement()
          statement.execute(ddl)
          logInfo("表已存在或建表成功!")
        }
      } else if (dbType == "doris") {
         logInfo("doris不支持建表")
      } else {
        throw new Exception(s"不支持的db类型: $dbType")
      }
      flag = false
    }
  }

  def main(args: Array[String]): Unit = {
    logInfo(s"args: ${args.toBuffer}")
    parseArgs(args)
    createTable()
    val properties = new Properties()
    properties.setProperty("url", connectionUrl)
    properties.setProperty("driver", driver)
    properties.setProperty("user", dbUser)
    properties.setProperty("password", dbPassword)
    val sparkConf = new SparkConf()
    val spark = SparkSession.builder()
      .appName("DBExporter")
      .enableHiveSupport()
      .config(sparkConf)
      .getOrCreate()
    val dataFrame = spark.sql(sql)
    val connection = DriverManager.getConnection(connectionUrl, dbUser, dbPassword)
    val statement = connection.createStatement()
    if (dbType == "clickhouse") {
      val ckPartition = partitions.split(",").map(p => {
        p.split("=")(1)
      })
      val dropPartition = s"alter table $targetTable drop partition ('${ckPartition.mkString("','")}')"
      logInfo(s"execute dml: $dropPartition")
      statement.execute(dropPartition)
      dataFrame.coalesce(5).write.option("batchsize", 10000000)
        .mode(SaveMode.Append).jdbc(connectionUrl, targetTable, properties)
    } else if (dbType == "mysql"){
      val whereClause = partitions.replaceAll(",", " AND ")
      val dropPartition = s"delete from $targetTable where $whereClause"
      logInfo(s"execute dml: $dropPartition")
      statement.execute(dropPartition)
      truncate = false
      val batchsize = sparkConf.get("spark.dbImporter.batchsize", "1000").toInt
      dataFrame.write.option("batchsize", batchsize).option("truncate", value = false)
        .mode(SaveMode.Append).jdbc(connectionUrl, targetTable, properties)
    } else if (dbType == "doris"){
      if (partitions.nonEmpty) {
        val whereClause = partitions.replaceAll(",", " AND ")
        val dropPartition = s"delete from $targetTable where $whereClause"
        logInfo(s"execute dml: $dropPartition")
        statement.execute(dropPartition)
      }
      val batchsize = sparkConf.get("spark.dbImporter.batchsize", "5000").toInt
      dataFrame.write.option("batchsize", batchsize).mode(SaveMode.Append).jdbc(connectionUrl, targetTable, properties)
    }
    spark.stop()

  }


}
