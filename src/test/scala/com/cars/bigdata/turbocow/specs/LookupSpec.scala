package com.cars.bigdata.turbocow

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.scalatest.junit.JUnitRunner
import com.cars.bigdata.turbocow.actions._
import org.apache.spark.sql.hive._

import scala.io.Source
import test.SparkTestContext._

class LookupSpec extends UnitSpec {

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

  val resourcesDir = "./src/test/resources/"

  describe("Lookup constructor") // ------------------------------------------------
  {
    it("should parse the config correctly (without a fromFile)") {

      implicit val formats = org.json4s.DefaultFormats
      val configStr =
        """
        {
          "activityType": "impressions",
          "items": [
            {
              "name": "lookup test", 
              "actions":[
                {
                  "actionType":"lookup",
                  "config": {
                    "select": [
                      "EnhField1",
                      "EnhField2"
                    ],
                    "fromDBTable": "testTable",
                    "where": "KEYFIELD",
                    "equals": "AField"
                  }
                }
              ]
            }
          ]
        }
        """

      val configAST = parse(configStr)
      val actionsList = ((configAST \ "items").children.head \ "actions")
      actionsList.children.size should be(1)

      // create the action and test all fields after construction:
      val actionConfig = actionsList.children.head \ "config"
      actionConfig should not be (JNothing)

      val action = Lookup(actionConfig, None)
      action.fromDBTable should be("testTable")
      action.fromFile should be(None)
      action.where should be("KEYFIELD")
      action.equals should be("AField")
      action.select should be(List("EnhField1", "EnhField2"))
    }

    it("should parse the config correctly with a fromFile") {

      implicit val formats = org.json4s.DefaultFormats
      val configStr =
        """
        {
          "activityType": "impressions",
          "items": [
            {
              "name": "lookup test", 
              "actions":[
                {
                  "actionType":"lookup",
                  "config": {
                    "select": [
                      "EnhField1",
                      "EnhField2"
                    ],
                    "fromDBTable": "testTable",
                    "fromFile": "./src/test/resources/testdimension-multirow.json",
                    "where": "KEYFIELD",
                    "equals": "AField"
                  }
                }
              ]
            }
          ]
        }
        """

      val configAST = parse(configStr)
      val actionsList = ((configAST \ "items").children.head \ "actions")
      actionsList.children.size should be(1)

      // create the action and test all fields after construction:
      val actionConfig = actionsList.children.head \ "config"
      actionConfig should not be (JNothing)

      val action = Lookup(actionConfig, None)
      action.fromDBTable should be("testTable")
      action.fromFile should be(Some("./src/test/resources/testdimension-multirow.json"))
      action.where should be("KEYFIELD")
      action.equals should be("AField")
      action.select should be(List("EnhField1", "EnhField2"))
    }
  }

  describe("Lookup action") {

    it("should successfully process one lookup") {
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new java.net.URI("./src/test/resources/input-integration.json"),
        """{
             "activityType": "impressions",
             "items": [
               {
                 "actions":[
                   {
                     "actionType":"lookup",
                     "config": {
                       "select": [
                         "EnhField1",
                         "EnhField2",
                         "EnhField3"
                       ],
                       "fromDBTable": "testTable",
                       "fromFile": "./src/test/resources/testdimension-multirow.json",
                       "where": "KEYFIELD",
                       "equals": "AField"
                     }
                   }
                 ]
               }
             ]
           }""".stripMargin,
        sc,
        Option(hiveCtx)).collect()

      enriched.size should be(1) // always one because there's only one json input object
      //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
      enriched.head("EnhField1") should be("1")
      enriched.head("EnhField2") should be("2")
      enriched.head("EnhField3") should be("3")
    }

    it("should successfully lookup non string values and convert to its String representation on the enrichedMap") {
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new java.net.URI("./src/test/resources/input-integration-lookupNonString.json"),
        """{
             "activityType": "impressions",
             "items": [
               {
                 "actions":[
                   {
                     "actionType":"lookup",
                     "config": {
                       "select": [
                         "IntTarget","FloatTarget","BooleanTarget"
                       ],
                       "fromDBTable": "testTable",
                       "fromFile": "./src/test/resources/test-nonstring-lookup.json",
                       "where": "AKey",
                       "equals": "AField"
                     }
                   }
                 ]
               }
             ]
           }""".stripMargin,
        sc,
        Option(hiveCtx)).collect()

      enriched.size should be(1) // always one because there's only one json input object
      println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched.head = "+enriched.head)
      enriched.head.size should be >= (4)
      enriched.head("IntTarget") should be("12")
      enriched.head("BooleanTarget") should be("true")
      enriched.head("FloatTarget") should be("12.3")
    }

    it("should correctly reject a record when the lookup fails") {
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new java.net.URI("./src/test/resources/input-integration-AA.json"), // 'AA' in AField
        """{
             "activityType": "impressions",
             "items": [
               {
                 "actions":[
                   {
                     "actionType":"lookup",
                     "config": {
                       "select": [
                         "EnhField1",
                         "EnhField2",
                         "EnhField3"
                       ],
                       "fromDBTable": "testTable",
                       "fromFile": "./src/test/resources/testdimension-multirow.json",
                       "where": "KEYFIELD",
                       "equals": "AField",
                       "onFail": [
                         { 
                           "actionType": "reject",
                           "config": {
                             "reasonFrom": "lookup"
                           }
                         }
                       ]
                     }
                   }
                 ]
               },
               {
                 "name": "final actions - MUST BE LAST!",
                 "actions":[
                   {
                     "actionType":"add-rejection-reason",
                     "config": {
                       "field": "reasonForReject"
                     }
                   }
                 ]
               }
             ]
           }""",
        sc,
        Option(hiveCtx)).collect()

      enriched.size should be(1) // always one because there's only one json input object
      //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)

      // test the record
      val recordMap = enriched.head
      val reasonOpt = recordMap.get("reasonForReject")
      reasonOpt.isEmpty should be(false)
      reasonOpt.get should be("Invalid KEYFIELD: 'AA'")
    }

    it("should correctly process lookup inside OnFail") {
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new java.net.URI("./src/test/resources/input-integration.json"),
        """{
             "activityType": "impressions",
             "items": [
               {
                 "actions":[
                   {
                     "actionType":"lookup",
                     "config": {
                       "select": [
                         "KEYFIELD"
                       ],
                       "fromDBTable": "testTable",
                       "fromFile": "./src/test/resources/testdimension-multirow.json",
                       "where": "KEYFIELD",
                       "equals": "BField",
                       "onFail": [
                         {
                           "actionType": "lookup",
                           "config": {
                              "select": [
                                   "KEYFIELD"
                              ],
                              "fromDBTable": "testTable",
                              "fromFile": "./src/test/resources/testdimension-multirow.json",
                              "where": "KEYFIELD",
                              "equals": "AField",
                              "onPass":[{
                                  "actionType" : "add-enriched-fields",
                                  "config" : [{
                                      "key" : "XYZ",
                                      "value" : "success"
                                  }]
                              }]
                           }
                         }
                       ]
                     }
                   }
                 ]
               }
             ]
           }""",
        sc,
        Option(hiveCtx)).collect()

      enriched.size should be(1) // always one because there's only one json input object
      //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)

      // test the record
      val recordMap = enriched.head
      recordMap("XYZ") should be("success")
    }

    it("should correctly process lookup inside OnPass") {
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new java.net.URI("./src/test/resources/input-integration.json"),
        """{
             "activityType": "impressions",
             "items": [
               {
                 "actions":[
                   {
                     "actionType":"lookup",
                     "config": {
                       "select": [
                         "KEYFIELD"
                       ],
                       "fromDBTable": "testTable",
                       "fromFile": "./src/test/resources/testdimension-multirow.json",
                       "where": "KEYFIELD",
                       "equals": "AField",
                       "onPass": [
                         {
                           "actionType": "lookup",
                           "config": {
                              "select": [
                                   "KEYFIELD"
                              ],
                              "fromDBTable": "testTable",
                              "fromFile": "./src/test/resources/testdimension-multirow.json",
                              "where": "KEYFIELD",
                              "equals": "BField",
                              "onFail":[{
                                  "actionType" : "add-enriched-fields",
                                  "config" : [{
                                      "key" : "XYZ",
                                      "value" : "failure"
                                  }]
                              }]
                           }
                         }
                       ]
                     }
                   }
                 ]
               }
             ]
           }""",
        sc,
        Option(hiveCtx)).collect()

      enriched.size should be(1) // always one because there's only one json input object
      //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)

      // test the record
      val recordMap = enriched.head
      recordMap("XYZ") should be("failure")
    }

  }

}




