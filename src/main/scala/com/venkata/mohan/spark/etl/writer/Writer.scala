package com.venkata.mohan.spark.etl.writer

import org.apache.spark.sql.DataFrame

trait Writer {
  def writeDataToHDFS(data: DataFrame, format: String, overwrite: String, separator: String, compression: String, path: String, header: Boolean, encoding: String): Unit

  def writeDataToLocal(path: String, data: String): Unit

  def writeDataToHDFSWithPartition(data: DataFrame, format: String = "csv", mode: String = "overwrite", separator: String = "\t", compression: String = "none", path: String,
                                   header: Boolean = false, encoding: String = "UTF-8", partitionColumn: String): Unit

  def writeDataToHiveTable(outputTable: String, data: DataFrame): Unit
}
