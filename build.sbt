
name := "etl"

version := "0.1"

scalaVersion := "2.11.11"

lazy val scalaTestVersion = "2.2.6"

lazy val sparkTestingBaseVersion = "2.1.0_0.8.0"

logLevel := Level.Warn

libraryDependencies ++= {
  val sparkVer = "2.1.0"
  val sqlVer = "2.1.0"

  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer,
    "org.apache.spark" %% "spark-sql" % sqlVer,
    "org.apache.spark" %% "spark-hive" % sqlVer,
    "org.slf4j" % "slf4j-log4j12" % "1.7.25",
    "com.typesafe" % "config" % "1.3.1"
  )
}


// Test
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % sparkTestingBaseVersion % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion % "test"

mainClass in assembly := Some("com.venkata.mohan.spark.etl.RunETL")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}