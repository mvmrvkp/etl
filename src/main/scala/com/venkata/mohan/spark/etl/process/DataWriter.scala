package com.venkata.mohan.spark.etl.process

import com.venkata.mohan.spark.etl.config.OutputTableConfigData
import com.venkata.mohan.spark.etl.util.Constants.HIVE_SOURCE_TYPE
import com.venkata.mohan.spark.etl.config.OutputTableConfigData
import com.venkata.mohan.spark.etl.writer.Writer
import org.apache.spark.sql.SparkSession

class DataWriter {
  def writeData(writer: Writer, outputTableConfigData: OutputTableConfigData)(implicit spark: SparkSession): Unit = {
    outputTableConfigData.destType match {
      case HIVE_SOURCE_TYPE => {
        println("Executing query "+outputTableConfigData.query)
        val data = spark.sql(outputTableConfigData.query)
        writer.writeDataToHiveTable(outputTableConfigData.tableName, data)
      }
      case _ => throw new Exception("Invalid source")
    }
  }
}
