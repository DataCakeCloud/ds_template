package com.ushareit.data.template.dbetl

import java.util.Base64
import java.util.regex.Pattern
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.ushareit.data.template.exception.DBExporterException
import com.ushareit.data.template.util.CreateTableUtil.{addColumnDDL, executeCreateTable, executeCreateTableByTendency, getDDL}
import com.ushareit.data.template.utils.EncryptUtil
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit

import scala.collection.mutable.ListBuffer
import scala.util.Try

object DBExporter extends Logging{

  private var connectionUrl: String = _
  private var dbUser: String = _
  private var dbPassword: String = ""
  private var sourceTable: String = _
  private var targetTable: String = _
  private var dbTabl: String = _
  private var partition: (String, String) = _
  private var driver = "com.mysql.jdbc.Driver"
  private var ddl = ""
  private var query = ""
  private var driverType = ""
  private var tempViewTableName = "dbetl_temp_view"
  private var sourceCols : List[Column] = _

  private def isBase64(str: String): Boolean = {
    val base64Pattern =
      "^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$"
    Pattern.matches(base64Pattern, str)
  }

  private def decode(str: String): String = {
    new String(Base64.getDecoder.decode(str.getBytes()))
  }

  // 查询固定字段，保障不会因为source表追加字段而导致任务失败
  private def generateDbTable(columns: List[Column], sourceTbl: String): String = {
    val selectCol = columns.map(col => {
      if("mysql".equals(driverType)){
        s"`${col.columnName}`"
      }else{
        col.columnName
      }
    }).distinct.sorted.reduce((x, y) => x + " , " + y).toString
    s"""
       |(select $selectCol from $sourceTbl) a
       |""".stripMargin
  }

  def parseArgsList(args:Array[String])={
//    assert(args.length > 1, "should set a json format args")
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val resultArgs = args(0)
    println(resultArgs)
    //    val config = mapper.readValue(resultArgs, classOf[List[DBEtlConfig]])
    val config: List[DBEtlConfig] = mapper.readValue(resultArgs, new TypeReference[List[DBEtlConfig]]() {
      /**/
    })
    val connectionUrl = config(0).connectionUrl
    driverType = connectionUrl.split(":")(1)
    driver = driverType match {
      case "mysql" => "com.mysql.jdbc.Driver"
      case "sqlserver" => "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      case "oracle" => "oracle.jdbc.driver.OracleDriver"
    }

    val dbListConfig: List[DBListConfig] = config.map(config => {
      if (config.partitions.nonEmpty) {
        partition = getPartition(config)
      }
      logInfo(s"select sql: ${config.sourceTable}")
//      val tempDdl = getDDL(columns, config)
      val tempTable = s"temp_${config.sourceTable.replaceAll("\\.","")}"
      // 生成查询sql语句
      val tempQuery = Try(generateSql(config.columns, tempTable)).get
      DBListConfig(config, partition, config.sourceTable, "", tempQuery,tempTable)
    })
    dbListConfig
  }

  def getPartition(config:DBEtlConfig)={
    val partitionKV = config.partitions.split(",")(0).split("=")
    var partitionValue = partitionKV(1)
    if (partitionValue.startsWith("'") || partitionValue.startsWith("\"")) {
      partitionValue = partitionValue.substring(1)
    }
    if (partitionValue.endsWith("'") || partitionValue.endsWith("\"")) {
      partitionValue = partitionValue.substring(0, partitionValue.length - 1)
    }
    (partitionKV(0), partitionValue)
  }


  def parseArgs(args: Array[String]): Unit = {
//    assert(args.length > 0, "should set a json format args")
    var json = ""
    if (isBase64(args(0))) {
      json = decode(args(0))
    } else {
      json = args(0)
    }
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val resultArgs = args(0)
    println(resultArgs)
    val config = mapper.readValue(resultArgs, classOf[DBEtlConfig])
    connectionUrl = config.connectionUrl
    driverType = connectionUrl.split(":")(1)
    driver = driverType match {
      case "mysql" => "com.mysql.jdbc.Driver"
      case "sqlserver" => "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      case "oracle" => "oracle.jdbc.driver.OracleDriver"
    }
    dbUser = config.dbUser
    dbPassword = config.dbPassword
    sourceTable = config.sourceTable
    targetTable = config.targetTable
    sourceCols = config.columns
    if (config.partitions.nonEmpty) {
      partition = getPartition(config)
    }

    logInfo(s"select sql: ${dbTabl}")

//    ddl = getDDL(columns,config)
    // 生成查询sql语句
    query = Try(generateSql(config.columns, tempViewTableName)).get
  }


  def generateSql(columns: List[Column], tbl: String): String = {
    var selectList = ListBuffer[String]()
    var whereList = ListBuffer[String]()
    columns.foreach(
      col => {
        val tupleResult = ColumnFuncGenerate.multiFuncsGenerate(col)
        selectList.append(tupleResult._1)
        whereList.append(tupleResult._2)
      }
    )
    var selectStr = selectList.filter(!_.trim.isEmpty).reduce(_ + ",\n" + _)
    var filterStr = ""
    if (whereList.filter(!_.trim.isEmpty).length > 0){
      filterStr = "and " + whereList.filter(!_.trim.isEmpty).reduce(_ + "\nand " + _)

    }


    val sql =
      s"""
         |select
         |$selectStr
         |from $tbl
         |where 1=1
         |$filterStr
         |""".stripMargin
    sql
  }


  def getTargetTbCol(spark:SparkSession,targetTable:String,partition:(String,String),sourceCols:List[Column]): String ={
    val descDf = spark.sql(s"desc $targetTable")
    descDf.show()
    val sqlDf = spark.sql(s"select * from $targetTable limit 0")

    val targetTblCols = sqlDf.columns
    targetTblCols.foreach(x=>log.info(s"targetTbl col:${x}"))

    var tempTargetTblCols = targetTblCols
    if(partition != null){
      tempTargetTblCols = targetTblCols.filter(x => !x.equals(partition._1))
    }
//    if(tempTargetTblCols.length != sourceCols.length ){
//      throw DBExporterException(s"The number of cols in the source table and target table is different,source:${sourceCols.length},target:${tempTargetTblCols.length} ")
//    }

    if(tempTargetTblCols.length == 1){
      tempTargetTblCols(0)
    }else{
      tempTargetTblCols.reduce((x,y) => s"$x,$y")
    }
  }

  def loadData(spark:SparkSession,url:String,driver:String,user:String,password:String,dbTable:String,sparkConf:SparkConf,fragmentationColumn:String)={
    var reader = spark.read
      .format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", EncryptUtil.decrypt(password, EncryptUtil.passwdEncryptKey))
      .option("dbtable", dbTable)

    val numPartitions = sparkConf.get("spark.dbExporter.numPartitions", "")
    var partitionColumn = sparkConf.get("spark.dbExporter.partitionColumn", "")
    val lowerBound = sparkConf.get("spark.dbExporter.lowerBound", "")
    val upperBound = sparkConf.get("spark.dbExporter.upperBound", "")

    if(partitionColumn.isEmpty){
      if (fragmentationColumn!=null && fragmentationColumn.nonEmpty){
        partitionColumn = fragmentationColumn
      }
    }

    // 通过查询自动填充上届、下届
    if (partitionColumn != null && partitionColumn.nonEmpty) {
      val ids = reader.option("dbtable", s"(select min($partitionColumn) as minId ,max($partitionColumn) as maxId from $dbTable) tmp")
        .load()
        .first()
      if (!ids.isNullAt(0)) {
        val minId = String.valueOf(ids.get(0)).toLong
        val maxId = String.valueOf(ids.get(1)).toLong
        val partitionNum = if (((maxId - minId) / 5000 + 1)>1000) 1000 else (maxId - minId) / 5000 + 1

        log.info(s"minId:$minId,maxId:$maxId,partitionNum:$partitionNum")

        reader = reader.option("dbtable", dbTable)
          .option("numPartitions", partitionNum)
          .option("partitionColumn", partitionColumn)
          .option("lowerBound", minId)
          .option("upperBound", maxId)
      }else{
        reader = reader.option("dbtable", dbTable)
      }
    }
    reader.load()
  }

  def executeTask(JDBCDataFrame:DataFrame,TargetTable:String,spark:SparkSession,partition:(String,String),
                  query:String,tempTable:String,sourceCols:List[Column])={

    JDBCDataFrame.createOrReplaceTempView(tempTable)

    log.info(s"targetTable:$TargetTable")

    // todo: 比较紧急,这里写的有点挫,之后待优化
    // 临时方案: -。-...
    // aws新加坡的目前就广告算法在用,并且使用的是parquet表,不支持动态插入数据,所以转换为sql采用静态插入
    if (TargetTable.startsWith("sg1")){
      // 去掉region前缀,不然找不到表 QAQ!!
      val targetTable = TargetTable.replace("sg1.","")
      // sql需要保证字段顺序与目标表一致,不然会乱序,所以先获取一次目标表的字段顺序,重新组装sql
      val targetColStr = getTargetTbCol(spark,targetTable,partition,sourceCols)
      var insertSql = ""
      // 重新组装sql
      if (partition != null){
        insertSql =
          s"""
             |insert overwrite $targetTable partition(${partition._1}='${partition._2}')
             |select $targetColStr
             |from ($query) a
             |""".stripMargin
      }else{
        insertSql =
          s"""
             |insert overwrite $targetTable
             |select $targetColStr
             |from ($query) a
             |""".stripMargin
      }

      log.info(s"execute insert sql: $insertSql")
      spark.sql(insertSql)

    }else{
      log.info(s"execute query sql: $query")
      val sqlDF = spark.sql(query)
      sqlDF.show(50)

      if (partition != null) {
        sqlDF.withColumn(partition._1, lit(partition._2)).writeTo(TargetTable).overwritePartitions()
      } else {
        sqlDF.writeTo(TargetTable).overwritePartitions()
      }
    }
  }




  def main(args: Array[String]): Unit = {
    logInfo(s"args: ${args.toBuffer}")
    val sparkConf = new SparkConf()
    val spark = SparkSession.builder()
      .appName("DBExporter")
      .enableHiveSupport()
      .config(sparkConf)
      .getOrCreate()
    logInfo(s"args length is : ${args.length}")
    if(args.length > 1){
      val configs = parseArgsList(args)

      if(args.length >= 3){
        configs.foreach(data => {
          val resultSql = getDDL(data.dbEtlConfig.columns, data.dbEtlConfig)
          logInfo("create table sql: " + resultSql)
          if(args.length ==3){
            executeCreateTable(resultSql, args(1), args(2))
          }else{
            executeCreateTableByTendency(resultSql, args(1), args(2),args(3))
          }
          logInfo("建表成功！！！")

          if(data.dbEtlConfig.addColumns!=null && data.dbEtlConfig.addColumns.nonEmpty&&data.dbEtlConfig.addColumns.size!=0){
            val changeDDl = addColumnDDL(data.dbEtlConfig.addColumns, data.dbEtlConfig)
            logInfo("alter table sql: "+ changeDDl)
            if (args.length == 3) {
              executeCreateTable(changeDDl, args(1), args(2))
            } else {
              executeCreateTableByTendency(changeDDl, args(1), args(2), args(3))
            }
            logInfo("添加字段成功！！！")
          }
        })
      }

      configs.foreach(config=>{
        logInfo(spark+"，"+config.dbEtlConfig.connectionUrl+"，"+driver+"，"+config.dbEtlConfig.dbUser+"，"+config.dbEtlConfig.dbPassword+"，"+config.dbTable)
        val JDBCDataFrame = loadData(spark,config.dbEtlConfig.connectionUrl,
          driver,
          config.dbEtlConfig.dbUser,
          config.dbEtlConfig.dbPassword,
          config.dbTable,
          sparkConf,
          config.dbEtlConfig.primaryKey)
        executeTask(JDBCDataFrame,config.dbEtlConfig.targetTable,spark,config.partition,
          config.query,config.tempTable,config.dbEtlConfig.columns)
      })
    }else{
      parseArgs(args)
      val JDBCDataFrame = loadData(spark,connectionUrl,driver,dbUser,dbPassword,sourceTable,sparkConf,"")
      executeTask(JDBCDataFrame,targetTable,spark,partition,query,tempViewTableName,sourceCols)
    }
   spark.stop()
  }

}
