package com.venkata.mohan.spark.etl.process

import com.venkata.mohan.spark.etl.config.OutputTableConfigData
import com.venkata.mohan.spark.etl.config.{InputTableConfigData, OutputTableConfigData}
import com.venkata.mohan.spark.etl.reader.ReaderImpl
import com.venkata.mohan.spark.etl.writer.WriterImpl
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

class ETLProcessor {

  def proceessETL(inputTableConfigDataList: ArrayBuffer[InputTableConfigData], outputTableConfigDataList: ArrayBuffer[OutputTableConfigData])(implicit spark: SparkSession): Unit = {
    val reader = new ReaderImpl
    val dataReader = new DataReader
    // step1 - Read data and register as temp tables
    inputTableConfigDataList.foreach(inputTableConfigData => {
      dataReader.readData(reader, inputTableConfigData)
    })

    // step2 - Write data to given destination
    val writer = new WriterImpl
    val dataWriter = new DataWriter
    outputTableConfigDataList.foreach(outputTableConfigData => {
      dataWriter.writeData(writer, outputTableConfigData)
    })
  }
}
