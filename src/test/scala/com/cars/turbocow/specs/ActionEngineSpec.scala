package com.cars.turbocow

import org.scalatest.junit.JUnitRunner
import com.cars.turbocow.actions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

import scala.io.Source
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

import com.databricks.spark.avro._

import scala.util.{Try, Success, Failure}

import java.io.File
import java.nio.file.Files

import SparkTestContext._

class ActionEngineSpec 
  extends UnitSpec 
  //with MockitoSugar 
{

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
      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration.json",
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
    
      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration.json",
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
      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration.json",
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
        ActionEngine.process(
          "./src/test/resources/input-integration.json",
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
        ActionEngine.process(
          "./src/test/resources/input-integration.json",
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
        ActionEngine.process(
          "./src/test/resources/input-integration.json",
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
        ActionEngine.process(
          "./src/test/resources/input-integration.json",
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

  describe("copy action") {
    it("should successfully process 'copy' with single config element") {

      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration.json",
        """{
          |  "activityType":"impressions",
          |  "items":[
          |    {
          |      "actions":[
          |        {
          |          "actionType":"copy",
          |          "config": [ 
          |            {
          |              "inputSource": "AField",
          |              "outputTarget": "AFieldEnriched"
          |            }
          |          ]
          |        }
          |      ]
          |    }
          |  ]
          |}""".stripMargin,
        sc).collect()

      enriched.size should be (1) // always one because there's only one json input object
      //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
      enriched.head.size should be (1)
      enriched.head.get("AFieldEnriched") should be (Some("A"))
    }

    it("should successfully process 'copy' with two config elements") {

      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration.json",
        """{
          |  "activityType":"impressions",
          |  "items":[
          |    {
          |      "actions":[
          |        {
          |          "actionType":"copy",
          |          "config": [ 
          |            {
          |              "inputSource": "AField",
          |              "outputTarget": "AFieldEnriched"
          |            },
          |            {
          |              "inputSource": "BField",
          |              "outputTarget": "BFieldEnriched"
          |            }
          |          ]
          |        }
          |      ]
          |    }
          |  ]
          |}""".stripMargin,
        sc).collect()

      enriched.size should be (1) // always one because there's only one json input object
      //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
      enriched.head.size should be (2)
      enriched.head.get("AFieldEnriched") should be (Some("A"))
      enriched.head.get("BFieldEnriched") should be (Some("B"))
    }

    it("should successfully 'copy' over blank values from the input record, if specified") {

      // Note: EField value is "" in the input record
      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration.json",
        """{
          |  "activityType":"impressions",
          |  "items":[
          |    {
          |      "actions":[
          |        {
          |          "actionType":"copy",
          |          "config": [ 
          |            {
          |              "inputSource": "AField",
          |              "outputTarget": "AFieldEnriched"
          |            },
          |            {
          |              "inputSource": "EField",
          |              "outputTarget": "EFieldEnriched"
          |            }
          |          ]
          |        }
          |      ]
          |    }
          |  ]
          |}""".stripMargin,
        sc).collect()

      enriched.size should be (1) // always one because there's only one json input object
      //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
      enriched.head.size should be (2)
      enriched.head.get("AFieldEnriched") should be (Some("A"))
      enriched.head.get("EFieldEnriched") should be (Some(""))
    }

    it("should throw exception on construct if 'copy' action has a null inputSource") {

      intercept[Exception] {
        ActionEngine.process(
          "./src/test/resources/input-integration.json",
          """{
          |    "activityType":"impressions",
          |    "items":[
          |      {
          |        "actions":[
          |          {
          |            "actionType":"copy",
          |            "config": [ 
          |              {
          |                "inputSource": null,
          |                "outputTarget": "AField"
          |              }
          |            ]
          |          }
          |        ]
          |      }
          |    ]
          |}""".stripMargin,
          sc
        )
      }
    }

    it("should throw exception on construct if 'copy' action has a null outputTarget") {

      intercept[Exception] {
        ActionEngine.process(
          "./src/test/resources/input-integration.json",
          """{
          |    "activityType":"impressions",
          |    "items":[
          |      {
          |        "actions":[
          |          {
          |            "actionType":"copy",
          |            "config": [ 
          |              {
          |                "inputSource": "AField",
          |                "outputTarget": null
          |              }
          |            ]
          |          }
          |        ]
          |      }
          |    ]
          |}""".stripMargin,
          sc
        )
      }
    }

    it("should throw exception on construct if 'copy' action has an empty inputSource") {

      intercept[Exception] {
        ActionEngine.process(
          "./src/test/resources/input-integration.json",
          """{
          |    "activityType":"impressions",
          |    "items":[
          |      {
          |        "actions":[
          |          {
          |            "actionType":"copy",
          |            "config": [ 
          |              {
          |                "inputSource": "",
          |                "outputTarget": "AField"
          |              }
          |            ]
          |          }
          |        ]
          |      }
          |    ]
          |}""".stripMargin,
          sc)
      }
    }

    it("should throw exception on construct if 'copy' action has a empty outputTarget") {

      intercept[Exception] {
        ActionEngine.process(
          "./src/test/resources/input-integration.json",
          """{
          |    "activityType":"impressions",
          |    "items":[
          |      {
          |        "actions":[
          |          {
          |            "actionType":"copy",
          |            "config": [ 
          |              {
          |                "inputSource": "AField",
          |                "outputTarget": ""
          |              }
          |            ]
          |          }
          |        ]
          |      }
          |    ]
          |}""".stripMargin,
          sc)
      }
    }

    it(" should throw an exception if 'copy' action does not have config object") {

      intercept[Exception] {
        ActionEngine.process(
          "./src/test/resources/input-integration.json",
          """{
            |  "activityType" : "impressions",
            |  "items" : [
            |    {
            |      "actions" : [
            |        {
            |          "actionType" : "copy"
            |        }
            |      ]
            |    }
            |  ]
            |}""".stripMargin,
          sc)
      }
    }
  }
  
  describe("replace null with") {

    // Helper test function
    def testReplaceNullWith(value: String) = {
      println("value = "+value)
      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration-replacenullwith.json",
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

  describe("custom actions") {

    it("should successfully process a custom action") {
      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration.json",
        """
          {
            "activityType": "impressions",
            "items": [
              {
                "actions":[
                  {
                    "actionType":"custom-add-enriched-fields",
                    "config": [
                      {
                        "key": "customA",
                        "value": "AAA"
                      },
                      {
                        "key": "customB",
                        "value": "BBB"
                      }
                    ]
                  }
                ]
              }
            ]
          }
        """,
        sc, 
        None,
        new ActionFactory(new CustomActionCreator) ).collect()
    
      enriched.size should be (1) // always one because there's only one json input object
      //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
      enriched.head.size should be (2)
      enriched.head("customA") should be ("AAA")
      enriched.head("customB") should be ("BBB")
    }
  }

  describe("misc ") {
    
    //it("should successfully process a different custom action") {
    //  val enriched: List[Map[String, String]] = ActionEngine.process(
    //    sc, 
    //    configFilePath = "./src/test/resources/testconfig-integration-custom2.json", 
    //    inputFilePath = "./src/test/resources/input-integration.json")
    //
    //  enriched.size should be (1) // always one because there's only one json input object
    //  //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
    //  enriched.head.get("EnhField1") should be (Some("1"))
    //  enriched.head.get("EnhField2") should be (None)
    //  enriched.head.get("EnhField3") should be (None)
    //}

    it("should process two items with the same source fields") {
      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration.json",
        fileToString("./src/test/resources/testconfig-2-items-same-source.json"),
        sc, 
        None,
        new ActionFactory(new CustomActionCreator) ).collect()
    
      enriched.size should be (1) // always one because there's only one json input object
      enriched.head("enrichedA") should be ("AAA")
      enriched.head("enrichedB") should be ("BBB")
      enriched.head("enrichedC") should be ("CCC")
      enriched.head("enrichedD") should be ("DDD")
      enriched.head("enrichedE") should be ("EEE")
      enriched.head("enrichedF") should be ("FFF")
    }
  }

  describe("reject action") {

    it("should collect the rejection reasons if more than one action calls reject") 
    {
      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration-AA.json", // 'AA' in AField
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
                         "EnhField2"
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
                 "actions":[
                   {
                     "actionType":"lookup",
                     "config": {
                       "select": [
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
                              "reason": "some reason text"
                            }
                          }
                       ]
                     }
                   }
                 ]
               }
             ]
           }""".stripMargin,
        sc, Some(hiveCtx)).collect()
    
      enriched.size should be (1) // always one because there's only one json input object
      //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
    
      // test the record
      val recordMap = enriched.head
      val reasonOpt = recordMap.get("reasonForReject")
      reasonOpt.isEmpty should be (false)
      reasonOpt.get should be ("Invalid KEYFIELD: 'AA'; some reason text")
    }
    
    it("should add nothing to the enriched record if lookup fails and no onFail is specified") {
      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration-AA.json", // 'AA' in AField
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
        sc, Some(hiveCtx)).collect()
    
      enriched.size should be (1) // always one because there's only one json input object
      //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
    
      // test the record
      val recordMap = enriched.head
      recordMap.size should be (0)
      recordMap should be (Map.empty[String, String])
    }

    it("should throw an exception when parsing the reject action with no config") {
    
      val e = intercept[Exception] {
        ActionEngine.process(
          "./src/test/resources/input-integration-AA.json", // 'AA' in AField
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
                     },
                     {
                       "actionType": "reject"
                     }
                   ]
                 }
               ]
             }""".stripMargin,
          sc, Some(hiveCtx)).collect()
      }
      e.getMessage should be ("'reject' actions should have either a 'reason' or 'reasonFrom' fields.  (Add one)")
    }

    it("should throw an exception when parsing the reject action with an empty config") {
    
      val e = intercept[Exception] {
        ActionEngine.process(
          "./src/test/resources/input-integration-AA.json", // 'AA' in AField
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
                     },
                     {
                       "actionType": "reject",
                       "config": {}
                     }
                   ]
                 }
               ]
             }""".stripMargin,
        sc, Some(hiveCtx)).collect()
      }
      e.getMessage should be ("'reject' actions should have either a 'reason' or 'reasonFrom' fields.  (Add one)")
    }

    it("should throw an exception when parsing a reject action with both reason fields") {
    
      val e = intercept[Exception] {
        ActionEngine.process(
          "./src/test/resources/input-integration-AA.json", // 'AA' in AField
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
                     },
                     {
                       "actionType": "reject",
                       "config": {
                         "reason": "Blah",
                         "reasonFrom": "Blah Blah"
                       }
                     }
                   ]
                 }
               ]
             }""".stripMargin,
        sc, Some(hiveCtx)).collect()
      }
    
      e.getMessage should be ("'reject' actions should not have both 'reason' and 'reasonFrom' fields.  (Pick only one)")
    }

    it("should output the entire input record if at least one action calls reject") 
    {
      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration-AA.json", // 'AA' in AField
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
               }
             ]
           }""".stripMargin,
        sc, Some(hiveCtx)).collect()
    
      enriched.size should be (1) // always one because there's only one json input object
      //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
    
      // test the record
      val recordMap = enriched.head
      recordMap.size should be (5)
      recordMap.get("reasonForReject") should be (Some("Invalid KEYFIELD: 'AA'"))
      recordMap.get("AField") should be (Some("AA"))
      recordMap.get("BField") should be (Some("B"))
      recordMap.get("CField") should be (Some("10"))
      recordMap.get("DField") should be (Some("11"))
    }

    it("should output all input fields but also any enriched fields from action lists BEFORE the rejection") 
    {
      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration-AA.json", // 'AA' in AField
        """{
             "activityType": "impressions",
             "items": [
               {
                 "actions":[
                   {
                     "actionType":"add-enriched-fields",
                     "config": [
                       {
                         "key": "X",
                         "value": "XVal"
                       },
                       {
                         "key": "Y",
                         "value": "YVal"
                       }
                     ]
                   }
                 ]
               },
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
               }
             ]
           }""".stripMargin,
        sc, Some(hiveCtx)).collect()
    
      enriched.size should be (1) // always one because there's only one json input object
    
      // test the record
      val recordMap = enriched.head
      recordMap.size should be (7)
      recordMap.get("reasonForReject") should be (Some("Invalid KEYFIELD: 'AA'"))
      recordMap.get("AField") should be (Some("AA"))
      recordMap.get("BField") should be (Some("B"))
      recordMap.get("CField") should be (Some("10"))
      recordMap.get("DField") should be (Some("11"))
      // these should be there from before the rejection:
      recordMap.get("X") should be (Some("XVal"))
      recordMap.get("Y") should be (Some("YVal"))
    }

    it("should output all input fields but also any enriched fields from action lists AFTER the rejection") 
    {
      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration-AA.json", // 'AA' in AField
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
                 "actions":[
                   {
                     "actionType":"add-enriched-fields",
                     "config": [
                       {
                         "key": "X",
                         "value": "XVal"
                       },
                       {
                         "key": "Y",
                         "value": "YVal"
                       }
                     ]
                   }
                 ]
               }
             ]
           }""".stripMargin,
        sc, Some(hiveCtx)).collect()
    
      enriched.size should be (1) // always one because there's only one json input object
    
      // test the record
      val recordMap = enriched.head
      recordMap.size should be (7)
      recordMap.get("reasonForReject") should be (Some("Invalid KEYFIELD: 'AA'"))
      recordMap.get("AField") should be (Some("AA"))
      recordMap.get("BField") should be (Some("B"))
      recordMap.get("CField") should be (Some("10"))
      recordMap.get("DField") should be (Some("11"))
      // these should be there from before the rejection:
      recordMap.get("X") should be (Some("XVal"))
      recordMap.get("Y") should be (Some("YVal"))
    }

    it("should stop processing on an action list by default") 
    {
      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration-AA.json", // 'AA' in AField
        """{
             "activityType": "impressions",
             "items": [
               {
                 "actions":[
                   {
                     "actionType":"reject",
                     "config": {
                       "reason": "because"
                     }
                   },
                   {
                     "actionType":"add-enriched-fields",
                     "config": [
                       {
                         "key": "X",
                         "value": "XVal"
                       },
                       {
                         "key": "Y",
                         "value": "YVal"
                       }
                     ]
                   }
                 ]
               }
             ]
           }""".stripMargin,
        sc).collect()
    
      enriched.size should be (1) // always one because there's only one json input object
    
      // test the record
      val recordMap = enriched.head
      recordMap.size should be (5)
      recordMap.get("reasonForReject") should be (Some("because"))
      recordMap.get("AField") should be (Some("AA"))
      recordMap.get("BField") should be (Some("B"))
      recordMap.get("CField") should be (Some("10"))
      recordMap.get("DField") should be (Some("11"))
      recordMap.get("X") should be (None) // (not really necessary)
      recordMap.get("Y") should be (None)
    }

    it("should not stop processing on an action list if specified in the config")
    {
      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration-AA.json", // 'AA' in AField
        """{
             "activityType": "impressions",
             "items": [
               {
                 "actions":[
                   {
                     "actionType":"reject",
                     "config": {
                       "reason": "because",
                       "stopProcessingActionList": false
                     }
                   },
                   {
                     "actionType":"add-enriched-fields",
                     "config": [
                       {
                         "key": "X",
                         "value": "XVal"
                       },
                       {
                         "key": "Y",
                         "value": "YVal"
                       }
                     ]
                   }
                 ]
               }
             ]
           }""".stripMargin,
        sc).collect()
    
      enriched.size should be (1) // always one because there's only one json input object
    
      // test the record
      val recordMap = enriched.head
      recordMap.size should be (7)
      recordMap.get("reasonForReject") should be (Some("because"))
      recordMap.get("AField") should be (Some("AA"))
      recordMap.get("BField") should be (Some("B"))
      recordMap.get("CField") should be (Some("10"))
      recordMap.get("DField") should be (Some("11"))
      // should be processed
      recordMap.get("X") should be (Some("XVal"))
      recordMap.get("Y") should be (Some("YVal"))
    }

    it("should stop processing on sub-actions as well as higher-level actions")
    {
      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration.json",
        """{
             "activityType": "impressions",
             "items": [
               {
                 "actions":[
                   {
                     "actionType":"lookup",
                     "config": {
                       "select": [
                         "EnhField1"
                       ],
                       "fromDBTable": "testTable",
                       "fromFile": "./src/test/resources/testdimension-multirow.json",
                       "where": "KEYFIELD",
                       "equals": "AField",
                       "onPass": [
                         {
                           "actionType": "reject",
                           "config": {
                             "reason": "because"
                           }
                         },
                         {
                           "actionType":"add-enriched-fields",
                           "config": [
                             {
                               "key": "SubX",
                               "value": "XVal"
                             }
                           ]
                         }
                       ]
                     }
                   },
                   {
                     "actionType":"add-enriched-fields",
                     "config": [
                       {
                         "key": "TopX",
                         "value": "XVal"
                       }
                     ]
                   }
                 ]
               }
             ]
           }""".stripMargin,
        sc, Some(hiveCtx)).collect()
    
      enriched.size should be (1) // always one because there's only one json input object
    
      // test the record
      val recordMap = enriched.head
      println("MMMMMMMMMMMMMMMMMMMMMM recordMap = "+recordMap)

      recordMap should be (
        Map(
          ("reasonForReject"-> "because"),
          ("AField"-> "A"),
          ("BField"-> "B"),
          ("CField"-> "10"),
          ("DField"-> "11"),
          ("EField"-> ""),
          ("EnhField1"-> "1")
        )
      )
    }

    it("should continue processing on sub-actions as well as higher-level actions (if requested)")
    {
      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration.json",
        """{
             "activityType": "impressions",
             "items": [
               {
                 "actions":[
                   {
                     "actionType":"lookup",
                     "config": {
                       "select": [
                         "EnhField1"
                       ],
                       "fromDBTable": "testTable",
                       "fromFile": "./src/test/resources/testdimension-multirow.json",
                       "where": "KEYFIELD",
                       "equals": "AField",
                       "onPass": [
                         {
                           "actionType": "reject",
                           "config": {
                             "reason": "because",
                             "stopProcessingActionList": false
                           }
                         },
                         {
                           "actionType":"add-enriched-fields",
                           "config": [
                             {
                               "key": "SubX",
                               "value": "XVal"
                             }
                           ]
                         }
                       ]
                     }
                   },
                   {
                     "actionType":"add-enriched-fields",
                     "config": [
                       {
                         "key": "TopX",
                         "value": "XVal"
                       }
                     ]
                   }
                 ]
               }
             ]
           }""".stripMargin,
        sc, Some(hiveCtx)).collect()
    
      enriched.size should be (1) // always one because there's only one json input object
    
      // test the record
      val recordMap = enriched.head

      recordMap should be (
        Map(
          ("reasonForReject"-> "because"),
          ("AField"-> "A"),
          ("BField"-> "B"),
          ("CField"-> "10"),
          ("DField"-> "11"),
          ("EField"-> ""),
          ("EnhField1"-> "1"),
          ("SubX"-> "XVal"),  // subX and topX should be there
          ("TopX"-> "XVal")
        )
      )
    }

  }

  describe("AvroOutputWriter") {
    it("should only output the fields in the schema regardless of what is in the input RDD") {
      val enriched: RDD[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration.json",
        """{
            "activityType": "impressions",
            "items": [
              {
                "actions":[{
                    "actionType":"simple-copy",
                    "config": {
                      "inputSource": [ "AField", "BField" ]
                    }
                  }
                ]
              }
            ]
          }
        """,
        sc).persist()
  
      // this should be the enriched record:

      val enrichedAll = enriched.collect()
      enrichedAll.size should be (1) // always one because there's only one json input object
      enrichedAll.head.size should be (2)
      enrichedAll.head.get("AField") should be (Some("A"))
      enrichedAll.head.get("BField") should be (Some("B"))

      // now write to avro
      //val tempFile = File.createTempFile("testoutput-", ".avro", null).deleteOnExit()
      val outputDir = { 
        val dir = Files.createTempDirectory("testoutput-")
        new File(dir.toString).delete()
        dir.toString
      }
      println("%%%%%%%%%%%%%%%%%%%%%%%%% outputDir = "+outputDir.toString)

      // write
      AvroOutputWriter.write(enriched, List("BField"), outputDir.toString, sc)

      // now read what we wrote
      val rows: Array[Row] = sqlCtx.read.avro(outputDir.toString).collect()
      rows.size should be (1) // one row only
      val row = rows.head
      row.size should be (1) // only one field in that row
      Try( row.getAs[String]("AField") ).isFailure should be (true)
      Try( row.getAs[String]("BField") ) should be (Success("B"))
    }
  }

  describe("getAllLookupRequirements") {
    it("should create a map with all the lookup requirements for each table in a map") {

      val testLookups = List(
        new Lookup(List("Select0"), "db.tableA", None, "whereA", "sourceField"),
        new Lookup(List("Select1"), "db.tableB", None, "whereB", "sourceField"),
        new Lookup(List("Select2"), "db.tableA", None, "whereA", "sourceField"),
        new Lookup(List("Select3"), "db.tableA", None, "whereA2", "sourceField"),
        new Lookup(List("Select4"), "db.tableA", None, "whereA3", "sourceField"),
        new Lookup(List("Select5", "Select5.1"), "db.tableB", None, "whereB", "sourceField")
      )
      val items = List(
        Item(
          actions = List(
            testLookups(0),
            testLookups(1)
          )
        ),
        Item(
          actions = List(
            testLookups(2),
            testLookups(3),
            testLookups(4),
            testLookups(5)
          )
        ) 
      )

      val reqs: List[CachedLookupRequirement] = ActionEngine.getAllLookupRequirements(items)

      reqs.size should be (2)
      reqs.foreach{ req =>

        req.dbTableName match {
          case "db.tableA" => {
            val keyFields = List("whereA", "whereA2", "whereA3")
            req.keyFields.sorted should be (keyFields.sorted)
            req.selectFields.sorted should be ((keyFields ++ List("Select0", "Select2", "Select3", "Select4")).sorted)
          }
          case "db.tableB" => {
            val keyFields = List("whereB")
            req.keyFields.sorted should be (List("whereB").sorted)
            req.selectFields.sorted should be ((keyFields ++ List("Select1", "Select5", "Select5.1")).sorted)
          }
          case _ => fail
        }
      }
    }
  }

/*
  describe("hive test") {
    it("should work locally") {
      import org.apache.spark.sql.hive.HiveContext

      val hiveCtx = new HiveContext(sc)

      val inputDF = hiveCtx.read.json("./src/test/resources/testdimension-multirow.json")
      inputDF.registerTempTable("dimtable")

      val rows = hiveCtx.sql("SELECT * FROM dimtable").collect

      println("here is the rowsDF from rowsDF.collect: ")
      rows.foreach{ r=> println(r.toString) }

      rows.size should be (3)
      rows(0).getAs[String]("KEYFIELD") should be ("A")
      rows(1).getAs[String]("KEYFIELD") should be ("B1")
      rows(2).getAs[String]("KEYFIELD") should be ("B2")
    }
  }
*/
}

 
