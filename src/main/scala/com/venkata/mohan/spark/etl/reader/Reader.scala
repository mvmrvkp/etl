package com.venkata.mohan.spark.etl.reader

import java.io.File

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Reader {

  def readDataFromHDFS(header: String = "false", delimiter: String = "\t", schema: StructType, path: String, nullValue: String = "null")(implicit spark: SparkSession): DataFrame

  def readDataFromHiveTable(dbName: String, tableName: String)(implicit spark: SparkSession): DataFrame

  def readDataFromHiveQuery(query: String)(implicit spark: SparkSession): DataFrame

  def readDataFromLocal(path: String): Array[String]

  def readJsonDataFromHDFS(path: String, schema: Option[StructType] = None)(implicit spark: SparkSession): DataFrame

  def getFilesByType(dirName: String, fileType: String): Array[File]

  def readTextFileFromHDFS(path: String)(implicit spark: SparkSession): String
}
