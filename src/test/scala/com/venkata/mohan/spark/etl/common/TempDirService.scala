package com.venkata.mohan.spark.etl.common

import java.io.File

import org.scalatest.{BeforeAndAfterEach, Suite}

trait TempDirService extends BeforeAndAfterEach {
  self: Suite =>

  var tempDir: File = _

  override def beforeEach(): Unit = {
    tempDir = File.createTempFile("com-rakuten-ichiba-solutions-adroll-", "")
    tempDir.delete()
    tempDir.mkdir()
    println(s"Created temp dir: ${tempDir.getAbsolutePath}")
  }

  override def afterEach(): Unit = {
    println(s"Deleting temp dir: ${tempDir.getAbsolutePath}")
    rm(tempDir)
  }

  def rm(file: File) {
    if (file.isDirectory) {
      Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(rm)
    }
    file.delete
  }
}

object TempDirService {

}