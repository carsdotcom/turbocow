name := "turbocow"

// Set the version.  Doing it this way makes it accessible later on via the val.
val ver = "0.7"
version := ver

// Set scalaVersion.   Doing it this way makes it accessible later on via the val.
val scalaVer = "2.10.6" // this is because of spark
scalaVersion := scalaVer

// Get just the Major.Minor version of scala for use elsewhere
val scalaVerMM = scalaVer.toString.split('.').take(2).mkString(".")

val json4SVer = "3.2.10" // don't use >= 3.3 due to conflicts
val sparkVer = "1.5.0" // NOTE this is due to cloudera (CDH 5.5.1)
//val sparkVer = "1.6.1" // for testing

// Always fork the jvm (test and run)
fork := true

// required for publish-local, in order to properly set groupId and organization in the POM:
organization := "com.cars.bigdata"

// Set this so we can run publish the POM and not error.
// Note that it gives deprecation warnings if you don't clean first.
val publishDir = "target/out"
publishTo := Some(Resolver.file("file", new File(publishDir)))

// Custom task to publish to Artifactory without username and password.
// Requires an API key that has write access to the repo, stored in an environment 
// variable called "ARTIFACTORY_KEY".
// Also requires 'curl' to be available (linux, cygwin/git-bash on windows, etc.)
val publishToArtifactory = taskKey[Unit]("publish the jar and POM to Artifactory.")
publishToArtifactory := {
  
  // Publish must be called to create the POM.
  publish.value
  
  import sys.process._
  import scala.language.postfixOps

  val artKey = sys.env.getOrElse("ARTIFACTORY_KEY", throw new Exception("Couldn't find environment variable ARTIFACTORY_KEY."))
  if (artKey.trim.isEmpty) throw new Exception("ARTIFACTORY_KEY environment variable was empty.")
  println("artKey = "+artKey)
  println("scalaVerMM = "+scalaVerMM)
  
  val pomFile = s"./target/scala-$scalaVerMM/turbocow_$scalaVerMM-$ver.pom"
  println("Pushing up the POM file:  "+pomFile)
  Seq("curl", "-v", 
    "-H", s"X-JFrog-Art-Api: $artKey", 
    "-T", pomFile, 
    s"https://repository.cars.com/artifactory/cars-data-local/com/cars/bigdata/turbocow_$scalaVerMM/$ver/turbocow_$scalaVerMM-$ver.pom"
  ).!!
  
  val jarFile = s"./target/scala-$scalaVerMM/turbocow_$scalaVerMM-$ver.jar"
  println("Pushing up the JAR file:  "+jarFile)
  Seq("curl", "-v", 
    "-H", s"X-JFrog-Art-Api: $artKey", 
    "-T", jarFile, 
    s"https://repository.cars.com/artifactory/cars-data-local/com/cars/bigdata/turbocow_$scalaVerMM/$ver/turbocow_$scalaVerMM-$ver.jar"
  ).!!
}

libraryDependencies ++= Seq(

   // spark
  "org.apache.spark" %% "spark-core" % sparkVer,
  "org.apache.spark" %% "spark-sql" % sparkVer,
  "org.apache.spark" %% "spark-hive" % sparkVer,
  "com.databricks" %% "spark-avro" % "2.0.1",

  // java libs
  "joda-time" % "joda-time" % "2.7",

  // For JSON parsing (see https://github.com/json4s/json4s)
  "org.json4s" %%  "json4s-jackson" % json4SVer,
  "org.json4s" %%  "json4s-ext" % json4SVer,
  
  // For testing:
  //"org.scalactic" %% "scalactic" % "2.2.6" % "test",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "org.mockito" % "mockito-all" % "1.10.19" % "test"

  // Note that this causes errors with the "-oF" test option below so it's 
  // disabled until I really need something in here:
  //"com.holdenkarau" %% "spark-testing-base" % (sparkVer + "_0.3.3") % "test"
)

// Print full stack traces in tests:
testOptions in Test += Tests.Argument("-oF")

