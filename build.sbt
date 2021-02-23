import sbt.util

name := "TaxiApp"

version := "0.1"

scalaVersion := "2.12.7"

logLevel := util.Level.Info

val sparkVersion = "2.4.3"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion

libraryDependencies += "com.lihaoyi" %% "ujson" % "1.2.2"
libraryDependencies += "com.lihaoyi" %% "requests" % "0.6.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % "test"
