package com.cars.bigdata.turbocow

import java.io.File
import java.net.URI
import java.nio.file.Files

import com.cars.bigdata.turbocow.actions._
import com.cars.bigdata.turbocow.test.SparkTestContext._
import com.databricks.spark.avro._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.io.Source
import scala.util.{Success, Try}

class ReplaceNullWithSpec 
  extends UnitSpec 
{
  val testTable = "testtable"

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
    super.afterEach()
  }

  // after all tests have run
  override def afterAll() = {
    super.afterAll()
  }

  /** Helper fn
    */
  def fileToString(filePath: String) = Source.fromFile(filePath).getLines.mkString

  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  // Tests start
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  
  describe("replace null with") {

    // Helper test function
    def testReplaceNullWith(value: String) = {
      println("value = "+value)
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration-replacenullwith.json"),
        s"""
        {
          "activityType": "impressions",
          "items": [
            {
              "actions":[
                {
                  "actionType": "replace-null-with-${value}",
                  "config": {
                    "inputSource": [ "AField", "CField", "DField" ]
                  }
                }
              ]
        
            }
          ]
        }""",
        sc).collect()

      enriched.size should be (1) // always one because there's only one json input object
      //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
      enriched.head.size should be (2)
      enriched.head.get("CField") should be (Some(value)) // this one was null
      enriched.head.get("DField") should be (Some(value)) // this one was missing
      enriched.head.get("AField") should be (None) // do nothing to a field that is not null
      // note the semantics of this are weird.  todo - rethink this action
    }

    it("should successfully process replace-null-with-X") {
      testReplaceNullWith("0")
      testReplaceNullWith("1")
      testReplaceNullWith("2")
      testReplaceNullWith("X")
      testReplaceNullWith("XXXXXYYYYZ have a nice day   ")
    }

    // todo this could use more testing
  }

}

 

