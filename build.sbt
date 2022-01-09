ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "AdidasScalaProject"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.12" % "2.4.0",
  "org.apache.spark" % "spark-sql_2.12" % "2.4.0",
  "org.apache.spark" % "spark-hive_2.12" % "2.4.0"
)
