package com.cars.bigdata.turbocow.specs

import com.cars.bigdata.turbocow.{test => _, _}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.scalatest.junit.JUnitRunner
import com.cars.bigdata.turbocow.actions._
import org.apache.spark.sql.hive._

import scala.io.Source
import com.cars.bigdata.turbocow.test.SparkTestContext._


/**
  * Created by nchaturvedula on 7/20/2016.
  */
class SearchAndReplaceSpec extends UnitSpec {


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

  describe("SearchAndReplace constructor") {

    it("should throw when inputSource is missing") {

      intercept[Exception] {
        val enriched: Array[Map[String, String]] = ActionEngine.processDir(
          new java.net.URI("./src/test/resources/input.json"),
          """{
          |   "activityType":"impressions",
          |   "items":[
          |      {
          |         "actions":[
          |            {
          |               "actionType":"search-and-replace",
          |               "config":{
          |                 "searchFor" : "any",
          |                 "replaceWith" : "anything"
          |               }
          |            }
          |         ]
          |      }
          |   ]
          |}""".
            stripMargin,
        sc).collect()
      }
    }

    it("should throw when inputSource is null") {

      intercept[Exception] {
        val enriched: Array[Map[String, String]] = ActionEngine.processDir(
          new java.net.URI("./src/test/resources/input.json"),
          """{
            |   "activityType":"impressions",
            |   "items":[
            |      {
            |         "actions":[
            |            {
            |               "actionType":"search-and-replace",
            |               "config":{
            |                 "inputSource" : null,
            |                 "searchFor" : "any",
            |                 "replaceWith" : "anything"
            |               }
            |            }
            |         ]
            |      }
            |   ]
            |}""".
            stripMargin,
          sc).collect()
      }


      }

    it("should throw when inputSource has an empty array") {

      intercept[Exception] {
        val enriched: Array[Map[String, String]] = ActionEngine.processDir(
          new java.net.URI("./src/test/resources/input.json"),
          """{
            |   "activityType":"impressions",
            |   "items":[
            |      {
            |         "actions":[
            |            {
            |               "actionType":"search-and-replace",
            |               "config":{
            |                 "inputSource" : [],
            |                 "searchFor" : "any",
            |                 "replaceWith" : "anything"
            |               }
            |            }
            |         ]
            |      }
            |   ]
            |}""".
            stripMargin,
          sc).collect()
      }

    }

    it("should throw when searchFor is missing") {

      intercept[Exception] {
        val enriched: Array[Map[String, String]] = ActionEngine.processDir(
          new java.net.URI("./src/test/resources/input.json"),
          """{
            |   "activityType":"impressions",
            |   "items":[
            |      {
            |         "actions":[
            |            {
            |               "actionType":"search-and-replace",
            |               "config":{
            |                 "inputSource" : ["A"],
            |
            |                 "replaceWith" : "anything"
            |               }
            |            }
            |         ]
            |      }
            |   ]
            |}""".
            stripMargin,
          sc).collect()
      }


  }

    it("should throw when searchFor is null") {

      intercept[Exception] {
        val enriched: Array[Map[String, String]] = ActionEngine.processDir(
          new java.net.URI("./src/test/resources/input.json"),
          """{
            |   "activityType":"impressions",
            |   "items":[
            |      {
            |         "actions":[
            |            {
            |               "actionType":"search-and-replace",
            |               "config":{
            |                 "inputSource" : ["A"],
            |                 "searchFor" : null,
            |                 "replaceWith" : "anything"
            |               }
            |            }
            |         ]
            |      }
            |   ]
            |}""".
            stripMargin,
          sc).collect()
      }


    }

    it("should throw when replaceWith is missing") {

      intercept[Exception] {
        val enriched: Array[Map[String, String]] = ActionEngine.processDir(
          new java.net.URI("./src/test/resources/input.json"),
          """{
            |   "activityType":"impressions",
            |   "items":[
            |      {
            |         "actions":[
            |            {
            |               "actionType":"search-and-replace",
            |               "config":{
            |                 "inputSource" : ["A"],
            |                 "searchFor" : "a"
            |               }
            |            }
            |         ]
            |      }
            |   ]
            |}""".
            stripMargin,
          sc).collect()
      }


    }

    it("should throw when replaceWith is null") {

      intercept[Exception] {
        val enriched: Array[Map[String, String]] = ActionEngine.processDir(
          new java.net.URI("./src/test/resources/input.json"),
          """{
            |   "activityType":"impressions",
            |   "items":[
            |      {
            |         "actions":[
            |            {
            |               "actionType":"search-and-replace",
            |               "config":{
            |                 "inputSource" : ["A"],
            |                 "searchFor" : "a",
            |                 "replaceWith" : null
            |               }
            |            }
            |         ]
            |      }
            |   ]
            |}""".
            stripMargin,
          sc).collect()
      }
    }
  }

  describe("SearchAndReplace Action"){

    def doSearchAndReplace(
      input : String,
      searchFor : String,
      replaceWith : String) :
      Array[Map[String, String]] ={

      ActionEngine.processJsonStrings(
        List(input),
        s"""{
          |   "activityType":"impressions",
          |   "items":[
          |      {
          |         "actions":[
          |            {
          |               "actionType":"search-and-replace",
          |               "config":{
          |                 "inputSource" : ["field"],
          |                 "searchFor" : "${searchFor}",
          |                 "replaceWith" : "${replaceWith}"
          |               }
          |            }
          |         ]
          |      }
          |   ]
          |}""".stripMargin,
        sc).collect()
    }

    it("should do nothing if input field is null"){
        val enrichedRecords = doSearchAndReplace(
          """{ "md": { "field": null } }""",
          "a",
          "A"
        )
      enrichedRecords.size should be (1)
      enrichedRecords.head("field") should be (null)
    }

    it(" should do nothing if input field is missing"){
      val enrichedRecords = doSearchAndReplace(
        """{ "md": { "fieldX": "Banana" } }""",
        "a",
        "A"
      )
      enrichedRecords.size should be (1)
      enrichedRecords.head.get("fieldX") should be (Some("Banana"))
    }

    it("should replace characters successfully") {
      val enrichedRecords = doSearchAndReplace(
        """{ "md": { "field": "Banana" } }""",
        "a",
        "~~~"
      )
      enrichedRecords.size should be (1)
      enrichedRecords.head.get("field") should be (Some("B~~~n~~~n~~~"))
    }

    it("should do nothing if characters are not found") {
      val enrichedRecords = doSearchAndReplace(
        """{ "md": { "field": "Banana" } }""",
        "X", // no X in Banana
        "~~~"
      )
      enrichedRecords.size should be (1)
      enrichedRecords.head.get("field") should be (Some("Banana"))
    }


  }
}
