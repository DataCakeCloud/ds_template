package com.ushareit.data.template.dbetl

import com.ushareit.data.template.`enum`.DBEtlFuncEnum
import com.ushareit.data.template.exception.DBExporterException
import org.apache.spark.internal.Logging

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

case class ColumnFuncGenerate()

object ColumnFuncGenerate extends Logging {

  private val mysqlNumberType = List("TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL")

  def multiFuncsGenerate(col: Column): Tuple2[String, String] = {
    var transformFuncsStrList = ListBuffer[String]()
    var filterFuncStrList = ListBuffer[String]()

    var transformFuncStr: String = ""
    var filterFuncStr: String = ""

    var colType = col.columnType.toLowerCase()

    if (col.funcs != null) {
      col.funcs.foreach(
        func => {
          transformFuncsStrList ++= funcGenerate(col.columnName, col.dataType, func)._1
          filterFuncStrList ++= funcGenerate(col.columnName, col.dataType, func)._2
        }
      )
    }

    if (transformFuncsStrList.length > 0) {
      // 拼接成 func4(func3(func2(func1,xxx,xxx),xxx,xxx),xxx,xxx),xxx,xxx)的格式
      val funcColStr = transformFuncsStrList.toList.reduce((x, y) => y.replace(col.columnName, s"$x"))
      transformFuncStr = s"cast($funcColStr as $colType) as ${col.columnName}"
    } else {
      transformFuncStr = s"cast(${col.columnName} as $colType) as ${col.columnName}"
    }

    // 拼接成 xxx  and  xxx的格式
    if (filterFuncStrList.length > 0) {
      filterFuncStr = filterFuncStrList.toList.reduce((x, y) => s"$x\nand $y")
    }

    (transformFuncStr, filterFuncStr)
  }


  def funcGenerate(colName: String, sourceType: String, func: Function): Tuple2[ListBuffer[String], ListBuffer[String]] = {
    var transformFuncsStrList = ListBuffer[String]()
    var filterFuncStrList = ListBuffer[String]()
    val enumTry = Try(DBEtlFuncEnum.withName(func.funcName))
    if (enumTry.isFailure) throw DBExporterException(s"get unknown function : '${func.funcName}'")

    enumTry.get match {
      case DBEtlFuncEnum.SUBSTR =>
        if (func.funcParams.length != 2) {
          throw DBExporterException(s"substr parameter error,need 2 but ${func.funcParams.length}")
        } else {
          transformFuncsStrList.append(substr(colName, func.funcParams(0), func.funcParams(1)))
        }
      case DBEtlFuncEnum.PAD =>
        if (func.funcParams.length != 3) {
          throw DBExporterException(s"pad parameter error,need 3 but ${func.funcParams.length}")
        } else {
          transformFuncsStrList.append(pad(colName, func.funcParams(0), func.funcParams(1), func.funcParams(2)))
        }
      case DBEtlFuncEnum.REPLACE =>
        if (func.funcParams.length != 2) {
          throw DBExporterException(s"replace parameter error,need 2 but ${func.funcParams.length}")
        } else {
          transformFuncsStrList.append(replace(colName, func.funcParams(0), func.funcParams(1)))
        }
      case DBEtlFuncEnum.FILTER =>
        if (func.funcParams.length != 2) {
          throw DBExporterException(s"filter parameter error,need 2 but ${func.funcParams.length}")
        } else {
          filterFuncStrList.append(filter(colName, sourceType,func.funcParams(0), func.funcParams(1)))
        }
    }
    (transformFuncsStrList, filterFuncStrList)
  }


  def substr(colName: String, startIndex: String, length: String): String = {
    s"substr( cast($colName as string) ,$startIndex,$length)"
  }

  def pad(colName: String, flag: String, targetLength: String, padCharacter: String): String = {
    if (!flag.toLowerCase.equals("l") && !flag.toLowerCase.equals("r")) {
      throw DBExporterException(s"parameter error, flag needs 'L' or 'R',but get '$flag'")
    }

    s"${flag.toLowerCase()}pad( cast($colName as string) ,$targetLength,'$padCharacter')"
  }

  def replace(colName: String, targetStr: String, repalceStr: String): String = {
    s"regexp_replace( cast($colName as string) ,'$targetStr','$repalceStr')"
  }


  def filter(colName: String, sourceType: String, operator: String, value: String): String = {
    val allOperator = List[String]("like", "not like", ">", "=", "<", ">=", "!=", "<=")
    if (!allOperator.contains(operator.trim.toLowerCase())) throw DBExporterException(s"parameter error,operator must in 'like,not like,>,=,<,>=,!=,<=' but '$operator' ")

    // 数值类型拼接
    if (mysqlNumberType.contains(sourceType.trim.toUpperCase)) {
      if (operator.equals("like") || operator.eq("not like")) {
        s"cast($colName as string) $operator '$value'"
      } else {
        s"$colName $operator $value"
      }
    } else {
      s"$colName $operator '$value'"
    }
  }
}
