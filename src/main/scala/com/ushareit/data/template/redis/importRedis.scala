package com.ushareit.data.template.redis

import java.net.URLDecoder
import java.text.SimpleDateFormat
import java.util
import java.util.regex.Pattern
import java.util.{Base64, Calendar, Date}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import redis.clients.jedis.{HostAndPort, JedisCluster}

object importRedis extends Logging{
  var redisAddr = ""
  var sql = ""
  var timeCategory = ""
  var redisPoolConf:String =_
  var interval:Int = _
  var redisPort:Int = _
  var setTtl:Boolean = _
  var currentTime:String = _

  def main(args: Array[String]): Unit = {
    args.sliding(2, 2).toList.collect {
      case Array("--sql", argSql:String) => sql = getDecodeSql(argSql)
      case Array("--redisAddr", argRedisAddr: String) => redisAddr = argRedisAddr
      case Array("--interval", argInterval: String) => interval = argInterval.toInt
      case Array("--timeCategory",argTimeCategory:String) => timeCategory = argTimeCategory
      case Array("--redisPort",argRedisPort:String) => redisPort = argRedisPort.toInt
      case Array("--redisPoolConf",argRedisPoolConf:String) => redisPoolConf = argRedisPoolConf
      case Array("--setTtl",argSetTtl:String) => setTtl = argSetTtl.toBoolean
      case Array("--currentTime",argCurrentTime:String) => currentTime = argCurrentTime.toString.replaceAll("@"," ")
    }
    assert(sql != null, "should set --sql")
    assert(redisAddr != null, "should set --redisAddr")
    logInfo(s"sql: ${sql}")
    println(s"${redisAddr}:${interval}:${timeCategory}:${redisPort}:${redisPoolConf}:${setTtl}:${currentTime}")
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .appName("importRedis")
      .getOrCreate()
    val mapBroad = new util.HashMap[String, String]()
    mapBroad.put("redisAddr",redisAddr);
    mapBroad.put("timeCategory",timeCategory)
    mapBroad.put("interval",interval.toString)
    mapBroad.put("redisPort",redisPort.toString)
    mapBroad.put("setTtl",setTtl.toString)
    mapBroad.put("currentTime",currentTime)
    val broadCast = spark.sparkContext.broadcast(mapBroad)
    val sparkDF = spark.sql(sql)
    val names: Array[String] = sparkDF.columns
    if(!names.contains("key") || !names.contains("value")){
      throw new Exception("请根据提示设置字段名称！")
    }
    if(sparkDF.schema.size > 3){
      throw new Exception("字段不能超过三个")
    }
    if(sparkDF.schema.size == 3){
      if(!names.contains("ttl")){
        throw new Exception("请根据提示设置时间别名")
      }
    }
    sparkDF
      .javaRDD
      .foreachPartition(
        data =>{
          val map: util.HashMap[String, String] = broadCast.value
          val cluster = new JedisCluster(new HostAndPort(map.get("redisAddr"), map.get("redisPort").toInt))
          while (data.hasNext){
            val row:Row = data.next()
            val key = if (row.getAs("key") == null) "NULl" else row.getAs("key").toString
            val value = if (row.getAs("value") == null) "NULL" else row.getAs("value").toString
            cluster.set(key,value)
            if(map.get("setTtl").toBoolean){
              if (row.size==3){
                val lastActiveDt = row.getAs[String]("ttl")
                if(lastActiveDt != null){
                  setExpireTime(key,lastActiveDt,map.get("interval").toInt,map.get("timeCategory"),cluster)
                }
              }else if(row.size==2){
                setExpireTime(key,map.get("currentTime"),map.get("interval").toInt,map.get("timeCategory"),cluster)
              }
            }
          }
          }
      )
    spark.stop()
  }

  def getDecodeSql(sql:String):String={
    val decoder = Base64.getDecoder
    URLDecoder.decode(new String(decoder.decode(sql), "utf-8"), "utf-8")
  }

  def setExpireTime(key: String, expireTime: String, interval: Int, formatter: String,jedisCluster:JedisCluster): Unit = {
    val dateMap: util.HashMap[String, Integer] = new util.HashMap[String, Integer]
    dateMap.put("day", Calendar.DATE)
    dateMap.put("hour", Calendar.HOUR)
    dateMap.put("minute", Calendar.MINUTE)
    dateMap.put("second",Calendar.SECOND)

    val calendarInstance: Calendar = Calendar.getInstance
    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat(getFormatter(expireTime))
    try {
      val parsedDate: Date = simpleDateFormat.parse(expireTime)
      calendarInstance.setTime(parsedDate)
      calendarInstance.add(dateMap.get(formatter), interval)
      val transformatedDate: Long = calendarInstance.getTime.toInstant.toEpochMilli
      jedisCluster.pexpireAt(key, transformatedDate)
    } catch {
      case e: Exception =>
        jedisCluster.del(key)
        throw new Exception("时间解析异常")
    }
  }

  def getFormatter(datetime: String): String = if (Pattern.matches("\\s*\\d{4}-\\d{2}-\\d{2}\\s*", datetime)) "yyyy-MM-dd" else "yyyy-MM-dd HH:mm:ss"
}
