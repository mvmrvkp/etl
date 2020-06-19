package com.venkata.mohan.spark.etl

import java.net.URL

import com.typesafe.config.ConfigFactory
import com.venkata.mohan.spark.etl.config.TableConfig
import com.venkata.mohan.spark.etl.process.ETLProcessor
import com.venkata.mohan.spark.etl.util.EtlUtil
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

//object RunETL extends App {

object RunETL {

  def runETL()(implicit spark: SparkSession): Unit= {
    val logger = LoggerFactory.getLogger(this.getClass.getName)
    logger.info("start etl processing")

    val etlConfig = ConfigFactory.load("application.conf")
    //val etlConfig = ConfigFactory.parseURL(new URL("http://localhost:8500/v1/kv/consulconfig/consul-config/etl.conf?raw=true")).withFallback(etlConfigFromFile)
    println("etlConfig  "+etlConfig)
    //etlConfig.entrySet().foreach(println)
    //val srcConfig = etlConfig.getConfig(("etl.input"))

    //println(srcConfig)
    //println("Source Table Name "+srcConfig.entrySet())
    //println(etlConfig)

    //srcConfig.entrySet().foreach(println)



    val inputTableConfigDataList = (etlConfig.getConfigList("etl.input") map (TableConfig.getInputTableConfigData(_))).to[ArrayBuffer]
    val outputTableConfigDataList = (etlConfig.getConfigList("etl.output") map (TableConfig.getOutputTableConfigData(_))).to[ArrayBuffer]
    println("tableConfigDataList "+inputTableConfigDataList)
    inputTableConfigDataList.foreach(x=>println("Table Config Data Table Name ["+x.tableName+"] Source Type ["+x.sourceType+"] query ["+x.query+"] path ["+x.path+"] Schema ["+x.schemaData+"]"))
    println("outputTableConfigDataList "+outputTableConfigDataList)
    outputTableConfigDataList.foreach(x => println("Output query "+x.query+" table  name "+x.tableName+" desttype "+x.destType))

    inputTableConfigDataList.foreach(tableConfigData => println("Schema "+EtlUtil.getSchema(tableConfigData.schemaData)))
/*

    tableConfigDataList.foreach()*/
    val eTLProcessor = new ETLProcessor
    eTLProcessor.proceessETL(inputTableConfigDataList, outputTableConfigDataList)
    logger.info("end etl processing")
  }

}
