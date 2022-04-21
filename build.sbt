ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "structuredStreamingKafkaIntegration"
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.1"
