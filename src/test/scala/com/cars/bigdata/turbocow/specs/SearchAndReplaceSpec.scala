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
    it("should successfully process searchFor-~-replaceWith-|") {

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
          |                 "inputSource" : ["adobe_id"],
          |                 "searchFor" : "~",
          |                 "replaceWith" : "|"
          |               }
          |            }
          |         ]
          |      }
          |   ]
          |}""".stripMargin,
        sc,
        None,
        new ActionFactory(new CustomActionCreator)).collect()


      enriched.head("adobe_id") should be ("[CS]v1|2B8185E9851937DD-40000605800EC3AC[CE]")
    }

    it("should successfully process searchFor any and replaceWith anything") {

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
          |                 "inputSource" : ["adobe_id2"],
          |                 "searchFor" : "any",
          |                 "replaceWith" : "anything"
          |               }
          |            }
          |         ]
          |      }
          |   ]
          |}""".stripMargin,
        sc).collect()


      enriched.head("adobe_id2") should be ("~~~anything~~~")
    }

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
          |                 "inputSource" : ["adobe"],
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

    //passing null when the field is missing or null in input json..can be configured to Empty if needed
    it(" should give null when adobe has null and found nothing to replace with"){
     doSearchAndReplace("""{ "adobe" : null }""" , "a" , "b").head("adobe") should be (null)
    }

    it(" should give null when adobe is missing and found nothing to replace with"){
      doSearchAndReplace("""{ }""" , "a" , "b").head("adobe") should be (null)
    }
  }
}
