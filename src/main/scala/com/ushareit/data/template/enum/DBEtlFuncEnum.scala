package com.ushareit.data.template.`enum`

import scala.util.{Failure, Success, Try}

object DBEtlFuncEnum extends Enumeration {
  type DBEtlFuncEnum = Value
  val SUBSTR = Value("substr")
  val PAD = Value("pad")
  val REPLACE = Value("replace")
  val FILTER = Value("filter")

  def isTransformFunc(func:DBEtlFuncEnum): Boolean = {
    func match {
      case SUBSTR | PAD | REPLACE => true
      case _ => false
    }
  }

  def isFilterFunc(func:DBEtlFuncEnum): Boolean = {
    func match {
      case FILTER => true
      case _ => false
    }
  }

  def checkExists(func:String) = this.values.exists(_.toString.equals(func))
  def showAll = this.values.foreach(println)

}
