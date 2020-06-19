package com.venkata.mohan.spark.etl

import com.venkata.mohan.spark.etl.common.FunSparkSuite
import com.venkata.mohan.spark.etl.reader.ReaderImpl
import com.venkata.mohan.spark.etl.schema.TestSchema
import com.venkata.mohan.spark.etl.udf.TestUdf
import com.venkata.mohan.spark.etl.util.EtlUtil
import com.venkata.mohan.spark.etl.writer.WriterImpl
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.scalatest.BeforeAndAfter

class RunETLTest extends FunSparkSuite with BeforeAndAfter{
  before {
    sqlContext.sql("""CREATE DATABASE test""".stripMargin)
    val reader = new ReaderImpl
    val writer = new WriterImpl
    val ngFilterDataPath = getClass.getResource("/data/default/hdfs/empdata").getPath
    val dataDf = reader.readDataFromHDFS(path = ngFilterDataPath, schema = TestSchema.empDataSchema)(spark)
    writer.writeDataToHiveTable("test.empdata", dataDf)
  }

  after {
    sqlContext.sql("""DROP TABLE test.empdata""".stripMargin)
    sqlContext.sql("""DROP TABLE test.empsaldata""".stripMargin)
    sqlContext.sql("""DROP TABLE test.empsaldatasecond""".stripMargin)
    sqlContext.sql("""DROP DATABASE test""".stripMargin)
  }

  test("Test Etl") {
    println("Start test")

    implicit val spark = SparkSession
      .builder()
      .appName("etl")//TODO - set propername
      .enableHiveSupport()
      .getOrCreate()

    // Register udfs
   spark.udf.register("incrementSalUdf", TestUdf.incrementSalUdf _)

    RunETL.runETL()
    val reader = new ReaderImpl
    val res1 = reader.readDataFromHiveTable(dbName = "test", tableName = "empsaldata")
    res1.printSchema()
    res1.show()
    val res2 = reader.readDataFromHiveTable(dbName = "test", tableName = "empsaldatasecond")
    res2.printSchema()
    res2.show()
    println("Success test")
  }

}
