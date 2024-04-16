package com.ushareit.data.template.util

import com.ushareit.data.template.dbetl.DBExporter.parseArgsList
import com.ushareit.data.template.dbetl.{Column, DBEtlConfig}
import io.prestosql.jdbc.PrestoResultSet
import org.apache.spark.internal.Logging

import java.sql.{Connection, DriverManager, SQLException, Statement}
import java.text.MessageFormat
import java.util.Properties

object CreateTableUtil extends Logging{

  private val DS_SPARK_CONF_DEMO =
    "--conf bdp-query-engine=spark-submit-sql-3\n" +
      "--conf bdp-query-user={0}\n" +
      "--conf bdp-query-region={1}\n" +
      "--conf spark.hive.exec.dynamic.partition.mode=nonstrict\n" +
      "--conf spark.hive.exec.dynamic.partition=true"

  private val DS_SPARK_CONF_DEMO_TENDENCY =
    "--conf bdp-query-engine=spark-submit-sql-3\n" +
      "--conf bdp-query-user={0}\n" +
      "--conf bdp-query-region={1}\n" +
      "--conf spark.hive.exec.dynamic.partition.mode=nonstrict\n" +
      "--conf spark.hive.exec.dynamic.partition=true\n" +
      "--conf bdp-query-tenancy={2}"

  private val OLAP_SERVICE_URL = "jdbc:presto://olap-service-direct-prod.ushareit.org:443/hive/default"

  private val COMMON_USER = "datastudio"

  private val COMMON_PASSWORD = "t2w3yZh3qG5v7MG"

  def main(args: Array[String]): Unit = {
    val configs = parseArgsList(args)

    println(args.length)

    configs.foreach(data => {

      val resultSql = getDDL(data.dbEtlConfig.columns, data.dbEtlConfig)
      logInfo("create table sql: " + resultSql)
      executeCreateTableByTendency(resultSql, args(1), args(2),args(3))
      logInfo("建表成功！！！")

//      println(addColumnDDL(data.dbEtlConfig.addColumns, data.dbEtlConfig))
//      println(data.dbEtlConfig.addColumns!=null)
//      if(data.dbEtlConfig.addColumns.isEmpty!=null){
//        println(data.dbEtlConfig.addColumns)
//      }
//      val resultSql = getDDL(data.dbEtlConfig.columns, data.dbEtlConfig)
//      logInfo("create table sql: "+resultSql)
//      executeCreateTable(addColumnDDL(data.dbEtlConfig.addColumns, data.dbEtlConfig),args(1),args(2))
//      logInfo("建表成功！！！")
    })

  }

//  def getCreateTableSql():{
//
//  }

  def getDDL(columns: List[Column], config: DBEtlConfig) = {
    val schema = columns.distinct.map(column => {
      val columnType = if (column.columnType.toLowerCase().startsWith("varchar")) {
        "string"
      } else {
        column.columnType
      }
      s"${column.columnName} $columnType COMMENT '${column.comment}'"
    }).mkString(", ")
    val location = config.location

    val targetTable = config.targetTable.replaceFirst("ue1|sg2|sg1", "iceberg")

    if(!config.partitions.isEmpty && config.partitions!=null){
      val partition = config.partitions.split("=")(0)
      val schemaAll = schema+ s", $partition STRING"
      s"CREATE TABLE IF NOT EXISTS $targetTable ($schemaAll) USING iceberg PARTITIONED BY (${config.partitions.split("=")(0)}) LOCATION '${location}'"
    }else{
      s"CREATE TABLE IF NOT EXISTS $targetTable ($schema) USING iceberg LOCATION '${location}'"
    }
  }

  def addColumnDDL(columns: List[Column],config: DBEtlConfig)={
    val addColumn = columns.map(column => {
      s"${column.columnName} ${column.columnType}"
    }).mkString(",")
    val targetTable = config.targetTable.replaceFirst("ue1|sg2|sg1", "iceberg")
    s"alter table $targetTable ADD COLUMNS ($addColumn)"
  }


  def executeCreateTable(sql:String,owner:String,region:String)={
    try {
      executeBySpark(sql, owner, region)
    } catch {
      case e: Exception =>
         logError("执行失败:"+e.getMessage)
    }

  }
  def executeCreateTableByTendency(sql:String,owner:String,region:String,tenancy:String)={
    try {
      executeBySparkByTendency(sql, owner, region,tenancy)
    } catch {
      case e: Exception =>
        logError("执行失败:" + e.getMessage)
    }
  }

  @throws[SQLException]
  def executeBySpark(sql: String, provider: String, region: String): String = {
    val conf = MessageFormat.format(DS_SPARK_CONF_DEMO, provider, region)
    val executeSql = conf + "\n" + sql
    execute(executeSql)
  }

  @throws[SQLException]
  def executeBySparkByTendency(sql: String, provider: String, region: String,tenancy:String): String = {
    val conf = MessageFormat.format(DS_SPARK_CONF_DEMO_TENDENCY, provider, region,tenancy)
    val executeSql = conf + "\n" + sql
    execute(executeSql)
  }


  private def getProperties = {
    val properties = new Properties
    properties.setProperty("user", COMMON_USER)
    properties.setProperty("password", COMMON_PASSWORD)
    properties.setProperty("SSL", "true")
    properties
  }

  @throws[SQLException]
  private def execute(sql: String) = {
    Class.forName("com.facebook.presto.jdbc.PrestoDriver")
    val properties = getProperties
    var connection: Connection = null
    var statement: Statement = null
    val sb = new StringBuffer
    try {
      connection = DriverManager.getConnection(OLAP_SERVICE_URL, properties)
      statement = connection.createStatement
      val rs = statement.executeQuery(sql)
      logInfo(sql)
      val queryId = rs.unwrap(classOf[PrestoResultSet]).getQueryId
      logInfo(queryId)
      while (rs.next) sb.append(rs.getString(1)).append("\n")
    } catch {
      case e: SQLException =>
        throw e
    } finally {
      try {
        statement.close()
        connection.close()
      } catch {
        case e: SQLException =>
          e.printStackTrace()
      }
    }
    sb.toString
  }


}
