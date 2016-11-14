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

import org.json4s._
import org.json4s.jackson.JsonMethods._

class SimpleCopySpec 
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
    if (hiveCtx.tableNames.contains(testTable)) hiveCtx.dropTempTable(testTable)
  }

  // after each test has run
  override def afterEach() = {
    super.afterEach()
  }

  // after all tests have run
  override def afterAll() = {
    super.afterAll()
    if (hiveCtx.tableNames.contains(testTable)) hiveCtx.dropTempTable(testTable)
  }

  /** Helper fn
    */
  def fileToString(filePath: String) = Source.fromFile(filePath).getLines.mkString

  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  // Tests start
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  describe("simple copy") // ------------------------------------------------
  {
    it("should successfully process one field") {
      val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
        Seq(s"""{ "md": { "AField": "A", "BField": "B" }, "activityMap": { "CField": 10, "DField": 11, "EField": "" }}"""),
        """{
            "activityType": "impressions",
            "items": [
              {
                "actions":[{
                    "actionType":"simple-copy",
                    "config": {
                      "inputSource": [ "AField" ]
                    }
                  }
                ]
              }
            ]
          }
        """,
        sc).collect()
  
      enriched.size should be (1) // always one because there's only one json input object
      enriched.head.size should be (1)
      enriched.head.get("AField") should be (Some("A"))
    }

    it("should successfully process two fields") {
    
      val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
        Seq(s"""{ "md": { "AField": "A", "BField": "B" }, "activityMap": { "CField": 10, "DField": 11, "EField": "" }}"""),
        """{
            "activityType": "impressions",
            "items": [
              {
                "actions":[{
                    "actionType":"simple-copy",
                    "config": {
                      "inputSource": [ "AField", "CField" ]
                    }
                  }
                ]
              }
            ]
          }
        """,
        sc).collect()
  
      enriched.size should be (1) // always one because there's only one json input object
      //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
      enriched.head.size should be (2)
      enriched.head.get("AField") should be (Some("A"))
      enriched.head.get("CField") should be (Some("10"))
    }

    it("should successfully copy over a field even if it is blank in the input") {
    
      // Note: EField is empty ("") in the input record
      val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
        Seq(s"""{ "md": { "AField": "A", "BField": "B" }, "activityMap": { "CField": 10, "DField": 11, "EField": "" }}"""),
        """{
            "activityType": "impressions",
            "items": [
              {
                "actions":[{
                    "actionType":"simple-copy",
                    "config": {
                      "inputSource": [ "AField", "EField", "CField" ]
                    }
                  }
                ]
              }
            ]
          }
        """,
        sc).collect()

      enriched.size should be (1) // always one because there's only one json input object
      //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
      enriched.head.size should be (3)
      enriched.head.get("AField") should be (Some("A"))
      enriched.head.get("CField") should be (Some("10"))
      enriched.head.get("EField") should be (Some(""))
    }

    it("should fail parsing missing config") {
      val e = intercept[Exception] {
        ActionEngine.processJsonStrings(
          Seq(s"""{ "md": { "AField": "A", "BField": "B" }, "activityMap": { "CField": 10, "DField": 11, "EField": "" }}"""),
          """{
              "activityType": "impressions",
              "items": [
                {
                  "actions":[{
                      "actionType":"simple-copy" }]}]}""",
          sc)
      }
    }
    it("should fail parsing empty list") {
      val e = intercept[Exception] {
        ActionEngine.processJsonStrings(
          Seq(s"""{ "md": { "AField": "A", "BField": "B" }, "activityMap": { "CField": 10, "DField": 11, "EField": "" }}"""),
          """{
              "activityType": "impressions",
              "items": [
                {
                  "actions":[{
                      "actionType":"simple-copy",
                      "config": {
                        "inputSource": [ ] }}]}]}""",
          sc)
      }
    }
    it("should fail parsing list with empty element") {
      val e = intercept[Exception] {
        ActionEngine.processJsonStrings(
          Seq(s"""{ "md": { "AField": "A", "BField": "B" }, "activityMap": { "CField": 10, "DField": 11, "EField": "" }}"""),
          """{
              "activityType": "impressions",
              "items": [
                {
                  "actions":[{
                      "actionType":"simple-copy",
                      "config": {
                        "inputSource": [ "A", "" ] }}]}]}""",
          sc)
      }
    }
    it("should fail parsing list with null element") {
      val e = intercept[Exception] {
        ActionEngine.processJsonStrings(
          Seq(s"""{ "md": { "AField": "A", "BField": "B" }, "activityMap": { "CField": 10, "DField": 11, "EField": "" }}"""),
          """{
              "activityType": "impressions",
              "items": [
                {
                  "actions":[{
                      "actionType":"simple-copy",
                      "config": {
                        "inputSource": [ "A", null ] }}]}]}""",
          sc)
      }
    }
  }
}

 

