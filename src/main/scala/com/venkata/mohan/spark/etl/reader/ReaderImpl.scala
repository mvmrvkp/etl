package com.venkata.mohan.spark.etl.reader

import java.io.File

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.io.Source

class ReaderImpl extends Reader {

  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  override def readDataFromHDFS(header: String = "false", delimiter: String = "\t", schema: StructType, path: String, nullValue: String = "null")
                               (implicit spark: SparkSession): DataFrame = {
    logger.info(s"Reading data from HDFS path: [$path] schema: ${schema.toString()} delimiter: $delimiter header: $header")
    spark.read.option("header", header).option("delimiter", delimiter).option("nullValue", nullValue).schema(schema).csv(path)
  }

  override def readDataFromHiveTable(dbName: String, tableName: String)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Reading data from hive database: $dbName tableName: $tableName")
    spark.sqlContext.table(s"$dbName.$tableName")
  }

  override def readDataFromHiveQuery(query: String)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Reading data from hive query: $query")
    spark.sqlContext.sql(query)
  }

  override def readDataFromLocal(path: String): Array[String] = {
    logger.info(s"Reading data from local path: $path")
    Source.fromFile(path).getLines().toArray
  }

  override def readJsonDataFromHDFS(path: String, schema: Option[StructType] = None)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Reading json data from HDFS path: [$path] schema:$schema")
    // schema is used when the reading json data is optional and we need to build the empty data frame with column names
    schema match {
      case Some(value) => spark.read.schema(schema = value).json(path)
      case None => spark.read.json(path)
    }
  }

  override def getFilesByType(dirName: String, fileType: String): Array[File] = {
    new java.io.File(dirName).listFiles.filter(_.getName.endsWith(s".$fileType"))
  }

  override def readTextFileFromHDFS(path: String)(implicit spark: SparkSession): String = {
    spark.sparkContext.textFile(path).collect()(0)
  }
}
