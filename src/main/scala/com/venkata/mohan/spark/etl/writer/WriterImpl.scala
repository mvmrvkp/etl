package com.venkata.mohan.spark.etl.writer

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

class WriterImpl extends Writer {

  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  private val QUOTE = "\u0000"

  override def writeDataToHDFS(data: DataFrame, format: String = "csv", mode: String = "overwrite", separator: String = "\t", compression: String = "none", path: String,
                               header: Boolean = false, encoding: String = "UTF-8"): Unit = {
    logger.info(s"Writing data to HDFS format: [$format] mode:[$mode] separator:[$separator] compression:[$compression] path:[$path] header:[$header] encoding:[$encoding]")
    data.write
      .format(format)
      .mode(mode)
      .option("sep", separator)
      .option("compression", compression)
      .option("header", header)
      .option("encoding", encoding)
      .option("quote", QUOTE)
      .save(path)
  }

  override def writeDataToLocal(path: String, data: String): Unit = {
    logger.info(s"Writing data to local path: [$path]")
    Files.write(Paths.get(path), data.getBytes(StandardCharsets.UTF_8))
  }

  override def writeDataToHDFSWithPartition(data: DataFrame, format: String = "csv", mode: String = "overwrite", separator: String = "\t", compression: String = "none", path: String,
                                            header: Boolean = false, encoding: String = "UTF-8", partitionColumn: String): Unit = {
    data.write.partitionBy(partitionColumn)
      .format(format)
      .mode(mode)
      .option("sep", separator)
      .option("compression", compression)
      .option("encoding", encoding)
      .option("quote", QUOTE)
      .save(path)
  }

  override def writeDataToHiveTable(outputTable: String, data: DataFrame): Unit = {
    logger.info(s"Writing data to hive table outputTable: [$outputTable]")
    data.write.saveAsTable(outputTable)
  }
}
