package com.ushareit.data.template.sendEmail

import java.io.ByteArrayOutputStream
import java.util.Base64
import java.util.zip.Inflater

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.ConfigFactory
import com.ushareit.data.template.dbetl.EmailConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import play.api.libs.mailer._

object sendEmail extends Logging{

  var from:String = _
  var toOther:String = _
  var subject:String = _
  var sql:String = _
  var text:String = _
  var from_passwd:String = _

  def parseArgs(args:String): Unit ={
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val config = mapper.readValue(args, classOf[EmailConfig])
    from = config.from
    toOther = config.to
    subject = config.subject
    sql = decode(config.sql.replaceAll(" ", ""))
    text = config.text
    from_passwd = config.from_password
    }

  def main(args: Array[String]): Unit = {
    parseArgs(args(0))
    log.info(s"--from ${from} --to $toOther --from_password ${from_passwd}  --subject $subject --sql $sql --text $text")

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val conf = spark.sparkContext.getConf
    val fileName = conf.get("spark.app.name", "data")
    val df = spark.sql(sql)
    df.show()


    if (toOther.isEmpty || toOther == null){
      throw new Exception("收件人不能为空")
    }
    val to = toOther.split(",").toList//邮件接收地址

    val bodyHtml = Option(
      s"""
        |<html>
        |<body>
        |<h4 align="left">${text}</h2>
        |</body>
        |</html>
      """.stripMargin)

    val charset = Option("utf-8")

    val name = fileName+".csv"
    val allData = df.schema.map(_.name).mkString(",") + "\n" +  df.rdd.map(_.mkString(",")).collect().mkString("\n")
    val data = allData.getBytes()
    val mimetype = "text/csv" // 根据文件类型选择MimeTypes对应的值
    val attachment: Attachment = AttachmentData(name, data, mimetype)
    val attachments: Seq[Attachment] = Seq(attachment)
    val email = Email(subject,from,to,None,bodyHtml,charset,attachments = attachments)

    // STMP服务参数
    val host = "smtp.qiye.aliyun.com" // STMP服务地址
    val port = 465 // STMP服务端口号
    val user = Option(from) // STMP服务用户邮箱
    val password = Option(from_passwd) // STMP服务邮箱密码
    val timeout = Option(60000) //setSocketTimeout 默认60s
    val connectionTimeout = Option(60000) //setSocketConnectionTimeout 默认60s

    val configuration: SMTPConfiguration = new SMTPConfiguration(
      host, port, true, false, false,
      user, password, false, timeout,
      connectionTimeout, ConfigFactory.empty(), false)
    val mailer: SMTPMailer = new SMTPMailer(configuration)

    // 发送邮件
    mailer.send(email)
    println("==========scala发邮件成功啦！！！==========")

    spark.stop()
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

}
