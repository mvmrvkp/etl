package com.venkata.mohan.spark.etl.common

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FunSuite, Matchers}

abstract class FunSparkSuite extends FunSuite
  with Matchers with DataFrameSuiteBase with TempDirService
