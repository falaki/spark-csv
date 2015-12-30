package com.databricks.spark.csv.util

private[csv] class Parameters(parameters: Map[String, String]) extends Serializable {

  def getChar(paramName: String, default: Option[Char] = None): Option[Char] = {
    val paramValue = parameters.get(paramName)
    paramValue match {
      case None => default
      case Some(value) => if (value.length == 1) {
        Some(value.charAt(0))
      } else {
        throw new RuntimeException(s"$paramName cannot be more than one character")
      }
    }
  }

  def getBool(paramName: String, default: Boolean = false): Boolean = {
    val param = parameters.getOrElse(paramName, default.toString)
    if (param.toLowerCase() == "true") {
      true
    } else if (param.toLowerCase == "false") {
      false
    } else {
      throw new Exception(s"$paramName flag can be true or false")
    }
  }

}