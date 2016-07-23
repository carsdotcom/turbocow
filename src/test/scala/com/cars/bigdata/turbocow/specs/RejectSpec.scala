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

class RejectSpec 
  extends UnitSpec 
  //with MockitoSugar 
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

  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  // Tests start
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  describe("reject action") {

    it("should collect the rejection reasons if more than one action calls reject") 
    {
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration-AA.json"), // 'AA' in AField
        s"""{
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
                       "fromDBTable": "$testTable",
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
                       "fromDBTable": "$testTable",
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
               ,{
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
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration-AA.json"), // 'AA' in AField
        s"""{
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
                       "fromDBTable": "$testTable",
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
        ActionEngine.processDir(
          new URI("./src/test/resources/input-integration-AA.json"), // 'AA' in AField
          s"""{
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
                         "fromDBTable": "$testTable",
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
        ActionEngine.processDir(
          new URI("./src/test/resources/input-integration-AA.json"), // 'AA' in AField
          s"""{
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
                         "fromDBTable": "$testTable",
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
        ActionEngine.processDir(
          new URI("./src/test/resources/input-integration-AA.json"), // 'AA' in AField
          s"""{
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
                         "fromDBTable": "$testTable",
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
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration-AA.json"), // 'AA' in AField
        s"""{
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
                       "fromDBTable": "$testTable",
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
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration-AA.json"), // 'AA' in AField
        s"""{
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
                       "fromDBTable": "$testTable",
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
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration-AA.json"), // 'AA' in AField
        s"""{
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
                       "fromDBTable": "$testTable",
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
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration-AA.json"), // 'AA' in AField
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
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration-AA.json"), // 'AA' in AField
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
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration.json"),
        s"""{
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
                       "fromDBTable": "$testTable",
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
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration.json"),
        s"""{
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
                       "fromDBTable": "$testTable",
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
}
