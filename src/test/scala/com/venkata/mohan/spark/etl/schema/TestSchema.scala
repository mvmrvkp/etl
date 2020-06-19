package com.venkata.mohan.spark.etl.schema

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object TestSchema {

val empDataSchema = StructType(Array(
  StructField("empid",DataTypes.IntegerType, false),
  StructField("empname", DataTypes.StringType, true)))

}
