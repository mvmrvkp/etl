package com.venkata.mohan.spark.etl.util

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import Constants._
import com.venkata.mohan.spark.etl.config.SchemaConfigData

import scala.collection.mutable.ArrayBuffer

object EtlUtil {

  def getSchema(schemaConfigDataList: ArrayBuffer[SchemaConfigData]): StructType={
    val SchemaFieldsList = schemaConfigDataList.map(schemaConfigData => {
      StructField(schemaConfigData.columnName,
        schemaConfigData.columnType match {
          case INTEGER_TYPE => DataTypes.IntegerType
          case DOUBLE_TYPE => DataTypes.DoubleType
          case FLOAT_TYPE => DataTypes.FloatType
          case BYTE_TYPE => DataTypes.ByteType
          case LONG_TYPE => DataTypes.LongType
          case STRING_TYPE => DataTypes.StringType
          case BINARY_TYPE => DataTypes.BinaryType
          case BOOLEAN_TYPE => DataTypes.BooleanType
          case DATE_TYPE => DataTypes.DateType
          case TIMESTAMP_TYPE => DataTypes.TimestampType
          case CALENDAR_INTERVAL_TYPE => DataTypes.CalendarIntervalType
          case NULL_TYPE => DataTypes.NullType
          case _ => throw new Exception("Invalid datatype")
        },
        schemaConfigData.isNullable
      )
    })
    StructType(SchemaFieldsList)
  }


  /*def registerDynamicUdfs(classFQN: String)(implicit spark: SparkSession):Unit = {
    val universe = scala.reflect.runtime.universe
    val runtimeMirror = universe(getClass.getClassLoader)
    val moduleSymbol = runtimeMirror.moduleSymbol(Class.forName(classFQN))
    var targetMethod = moduleSymbol.typeSignature.members.filter(x =>
      x.isMethod && x.name.toString.contains("Udf")).head.asMethod
    var function = mirror.reflect(mirror.reflectModule(moduleSymbol).instance).reflectMethod(targetMethod)()
    spark.udf.register(targetMethod.name.toString, function)
  }*/

  /*def registerDynamicUdfs(classFQN: String)(implicit spark: SparkSession) =
  {
    val universe = scala.reflect.runtime.universe
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val moduleSymbol = runtimeMirror.moduleSymbol(Class.forName(classFQN))
    var targetMethod = moduleSymbol.typeSignature.members.filter(x =>
      x.isMethod && x.name.toString.contains("Udf")).head.asMethod
    var function = runtimeMirror.reflect(runtimeMirror.reflectModule(moduleSymbol).instance).reflectMethod(targetMethod)
    //spark.udf.register(udfName, function)
    //val udf1 = udf(function, DataTypes.DoubleType)
    val test = incrementSalUdf _
    println(test)
    spark.udf.register(targetMethod.name.toString, test)
  }*/

/*
  def registerDynamicUdfs(classFQN: String)(implicit spark: SparkSession) =
  {
    val universe = scala.reflect.runtime.universe
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val moduleSymbol = runtimeMirror.moduleSymbol(Class.forName(classFQN))
    var targetMethod = moduleSymbol.typeSignature.members.filter(x =>
      x.isMethod && x.name.toString.contains("Udf")).head.asMethod
    var function = runtimeMirror.reflect(runtimeMirror.reflectModule(moduleSymbol).instance).reflectMethod(targetMethod)
    val test = function.
    println(test)
    println(targetMethod.name.toString)
    spark.udf.register(targetMethod.name.toString, test)
  }*/


}