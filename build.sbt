name := "turbocow"

// required for publish-local, in order to properly set groupId and organization in the POM:
organization := "com.cars.bigdata"

version := "0.3"

scalaVersion := "2.10.6" // this is because of spark

val json4SVer = "3.2.10" // don't use >= 3.3 due to conflicts
val sparkVer = "1.5.0" // NOTE this is due to cloudera (CDH 5.5.1)
//val sparkVer = "1.6.1" // for testing

libraryDependencies ++= Seq(

   // spark
  "org.apache.spark" %% "spark-core" % sparkVer,
  "org.apache.spark" %% "spark-sql" % sparkVer,
  "org.apache.spark" %% "spark-hive" % sparkVer,
  "com.databricks" %% "spark-avro" % "0.1",

  // java libs
  "joda-time" % "joda-time" % "2.7",

  // For JSON parsing (see https://github.com/json4s/json4s)
  "org.json4s" %%  "json4s-jackson" % json4SVer,
  "org.json4s" %%  "json4s-ext" % json4SVer,
  
  // For testing:
  //"org.scalactic" %% "scalactic" % "2.2.6" % "test",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "org.mockito" % "mockito-all" % "1.10.19" % "test"
)

// Print full stack traces in tests:
testOptions in Test += Tests.Argument("-oF")

