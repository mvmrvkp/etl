package com.venkata.mohan.spark.etl.config

import com.typesafe.config.Config

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object TableConfig {

  def getInputTableConfigData(params : Config):InputTableConfigData={

    //println(params.getConfigList("schema"))

    val SchemaConfigDataList = if(params.hasPath("schema")) (params.getConfigList("schema") map (getSchemaConfigData(_))).to[ArrayBuffer] else ArrayBuffer.empty[SchemaConfigData]
    println("SchemaConfigDataList "+SchemaConfigDataList)


    return InputTableConfigData(params.getString("tableName"), params.getString("sourceType"),
      if(params.hasPath("query")) params.getString("query") else "",
      if(params.hasPath("path")) params.getString("path") else "",
      SchemaConfigDataList)
  }


  def getSchemaConfigData(params : Config): SchemaConfigData = {
    return SchemaConfigData(
      if(params.hasPath("columnName")) params.getString("columnName") else "",
      if(params.hasPath("dataType")) params.getString("dataType") else "",
      if(params.hasPath("isnullable")) params.getBoolean("isnullable") else true)
  }
  /*implicit class RichConfig(val underlying: Config) extends AnyVal {
    def getOptionalBoolean(path: String): Option[Boolean] = if (underlying.hasPath(path)) {
      Some(underlying.getBoolean(path))
    } else {
      None
    }
  }*/


  def getOutputTableConfigData(params : Config):OutputTableConfigData={
    return OutputTableConfigData(params.getString("tableName"),
      params.getString("destType"),
      if(params.hasPath("query")) params.getString("query") else "",
      if(params.hasPath("path")) params.getString("path") else "")
  }
}

case class InputTableConfigData(tableName: String, sourceType: String, query: String, path: String, schemaData: ArrayBuffer[SchemaConfigData])
case class SchemaConfigData(columnName: String, columnType: String, isNullable: Boolean)
case class OutputTableConfigData(tableName: String, destType: String, query: String, path: String)