package com.venkata.mohan.spark.etl.process

import com.venkata.mohan.spark.etl.util.Constants._
import com.venkata.mohan.spark.etl.config.InputTableConfigData
import com.venkata.mohan.spark.etl.reader.Reader
import com.venkata.mohan.spark.etl.util.EtlUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataReader {

  def readData(reader: Reader, inputTableConfigData: InputTableConfigData)(implicit spark: SparkSession): Unit ={
    inputTableConfigData.sourceType match {
      case HIVE_SOURCE_TYPE => {
        val data = reader.readDataFromHiveQuery(inputTableConfigData.query)
        data.createOrReplaceTempView(inputTableConfigData.tableName)
        printInfo(data)
      }

      case HDFS_SOURCE_TYPE => {
        println("schema "+EtlUtil.getSchema(inputTableConfigData.schemaData))
        val data = reader.readDataFromHDFS(schema = EtlUtil.getSchema(inputTableConfigData.schemaData), path = inputTableConfigData.path)
        data.createOrReplaceTempView(inputTableConfigData.tableName)
        printInfo(data)
      }

      case _ => throw new Exception("Invalid source")
    }
  }

  def printInfo(df:DataFrame): Unit ={
    df.printSchema()
    df.count()
    df.show()
  }

}
