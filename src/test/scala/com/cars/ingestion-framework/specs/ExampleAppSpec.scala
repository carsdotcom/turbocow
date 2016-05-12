package com.cars.ingestionframework.exampleapp

import org.scalatest.junit.JUnitRunner
import com.cars.ingestionframework._
import com.cars.ingestionframework.actions._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// Fix for Scalatest on Gradle:  (from http://stackoverflow.com/questions/18823855/cant-run-scalatest-with-gradle)
// Alternately, try using https://github.com/maiflai/gradle-scalatest
//@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ExampleAppSpec extends UnitSpec {

  // before all tests have run
  override def beforeAll() = {
    super.beforeAll()
  }

  // before each test has run
  override def beforeEach() = {
    super.beforeEach()
  }

  // after each test has run
  override def afterEach() = {
    //myAfterEach()
    super.afterEach()
  }

  // after all tests have run
  override def afterAll() = {
    super.afterAll()
  }

  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  // Tests start
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  describe("enrich()")  // ------------------------------------------------
  {
    it("should successfully process simple-copy") {
    
      // initialise spark context
      val conf = new SparkConf().setAppName("ExampleApp").setMaster("local[1]")
      val sc = new SparkContext(conf)
      try {
        val enriched: List[Map[String, String]] = ExampleApp.enrich(
          sc, 
          configFilePath = "./src/test/resources/testconfig-integration-simplecopy.json", 
          inputFilePath = "./src/test/resources/input-integration.json")
    
        enriched.size should be (1) // always one because there's only one json input object
        //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
        enriched.head("AField") should be ("A")
        enriched.head("CField") should be ("10")
      }
      finally {
        // terminate spark context
        sc.stop()
      }
    }

    /** Helper test function
      */
    def testReplaceNullWith(value: Int) = {
      // initialise spark context
      val conf = new SparkConf().setAppName("ExampleApp").setMaster("local[1]")
      val sc = new SparkContext(conf)
      
      try {
        val enriched: List[Map[String, String]] = ExampleApp.enrich(
          sc, 
          configFilePath = s"./src/test/resources/testconfig-integration-replacenullwith${value.toString}.json", 
          inputFilePath = "./src/test/resources/input-integration-replacenullwith.json")

        enriched.size should be (1) // always one because there's only one json input object
        //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
        enriched.head("AField") should be ("A")
        enriched.head("CField") should be (value.toString) // this one was null
        enriched.head("DField") should be (value.toString) // this one was missing
      }
      finally {
        // terminate spark context
        sc.stop()
      }
    }

    it("should successfully process replace-null-with-0") {
      testReplaceNullWith(0)
    }

    // make sure any value will work
    it("should successfully process replace-null-with-999") {
      testReplaceNullWith(999)
    }

  }
}




