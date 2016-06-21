package com.cars.turbocow

import org.scalatest.junit.JUnitRunner
import com.cars.turbocow.actions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

import scala.io.Source
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

class ActionEngineSpec 
  extends UnitSpec 
  //with MockitoSugar 
{

  // initialise spark context
  //val conf = new SparkConf().setAppName("ActionEngineSpec").setMaster("local[1]")
  val conf = new SparkConf().setAppName("ActionEngineSpec").setMaster("local[2]")
  val sc = new SparkContext(conf)

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

    // stop spark
    sc.stop()
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
                      "source": [ "AField" ]
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
                      "source": [ "AField", "CField" ]
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
                      "source": [ "AField", "EField", "CField" ]
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
                        "source": [ ] }}]}]}""",
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
                        "source": [ "A", "" ] }}]}]}""",
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
                        "source": [ "A", null ] }}]}]}""",
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
          |              "sourceName": "AField",
          |              "enrichedName": "AFieldEnriched"
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
          |              "sourceName": "AField",
          |              "enrichedName": "AFieldEnriched"
          |            },
          |            {
          |              "sourceName": "BField",
          |              "enrichedName": "BFieldEnriched"
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
          |              "sourceName": "AField",
          |              "enrichedName": "AFieldEnriched"
          |            },
          |            {
          |              "sourceName": "EField",
          |              "enrichedName": "EFieldEnriched"
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

    it("should throw exception on construct if 'copy' action has a null sourceName") {

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
          |                "sourceName": null,
          |                "enrichedName": "AField"
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

    it("should throw exception on construct if 'copy' action has a null enrichedName") {

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
          |                "sourceName": "AField",
          |                "enrichedName": null
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

    it("should throw exception on construct if 'copy' action has an empty sourceName") {

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
          |                "sourceName": "",
          |                "enrichedName": "AField"
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

    it("should throw exception on construct if 'copy' action has a empty enrichedName") {

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
          |                "sourceName": "AField",
          |                "enrichedName": ""
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
                    "source": [ "AField", "CField", "DField" ]
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

  describe("lookup action") {

    it("should successfully process one lookup") {
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
                       "lookupFile": "./src/test/resources/testdimension-table-for-lookup.json",
                       "lookupField": "KEYFIELD",
                       "lookupFieldValue": "AField",
                       "fieldsToSelect": [
                         "EnhField1",
                         "EnhField2",
                         "EnhField3"
                       ]
                     }
                   }
                 ]
               }
             ]
           }""".stripMargin,
        sc).collect()
  
      enriched.size should be (1) // always one because there's only one json input object
      //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
      enriched.head("EnhField1") should be ("1")
      enriched.head("EnhField2") should be ("2")
      enriched.head("EnhField3") should be ("3")
    }

    it("should correctly reject a record when the lookup fails") {
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
                       "lookupFile": "./src/test/resources/testdimension-table-for-lookup.json",
                       "lookupField": "KEYFIELD",
                       "lookupFieldValue": "AField",
                       "fieldsToSelect": [
                         "EnhField1",
                         "EnhField2",
                         "EnhField3"
                       ],
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
           }""",
        sc).collect()
    
      enriched.size should be (1) // always one because there's only one json input object
      //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
    
      // test the record
      val recordMap = enriched.head
      recordMap.size should be (1)
      val reasonOpt = recordMap.get("reasonForReject")
      reasonOpt.isEmpty should be (false)
      reasonOpt.get should be ("Invalid KEYFIELD: 'AA'")
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
                       "lookupFile": "./src/test/resources/testdimension-table-for-lookup.json",
                       "lookupField": "KEYFIELD",
                       "lookupFieldValue": "AField", 
                       "fieldsToSelect": [
                         "EnhField1",
                         "EnhField2"
                       ],
                       "onFail": [
                          {
                            "actionType": "reject",
                            "config": {
                              "reasonFrom": "lookup"
                            }
                          }
                       ]
                     }
                   },
                   {
                     "actionType":"lookup",
                     "config": {
                       "lookupFile": "./src/test/resources/testdimension-table-for-lookup.json",
                       "lookupField": "KEYFIELD",
                       "lookupFieldValue": "AField", 
                       "fieldsToSelect": [
                         "EnhField3"
                       ],
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
        sc).collect()
    
      enriched.size should be (1) // always one because there's only one json input object
      //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
    
      // test the record
      val recordMap = enriched.head
      recordMap.size should be (1)
      val reasonOpt = recordMap.get("reasonForReject")
      reasonOpt.isEmpty should be (false)
      reasonOpt.get should be ("Invalid KEYFIELD: 'AA'; some reason text")
    }
    
    //it("should output the entire input record if at least one action calls reject") {
    //  val enriched: Array[Map[String, String]] = ActionEngine.process(
    //    "./src/test/resources/input-integration-AA.json", // 'AA' in AField
    //    """{
    //         "activityType": "impressions",
    //         "items": [
    //           {
    //             "source": [ "AField" ], 
    //             "actions":[
    //               {
    //                 "actionType":"lookup",
    //                 "config": {
    //                   "lookupFile": "./src/test/resources/testdimension-table-for-lookup.json",
    //                   "lookupField": "KEYFIELD",
    //                   "fieldsToSelect": [
    //                     "EnhField1",
    //                     "EnhField2",
    //                     "EnhField3"
    //                   ],
    //                   "onFail": [
    //                      {
    //                        "actionType": "reject",
    //                        "config": {
    //                          "reasonFrom": "lookup"
    //                        }
    //                      }
    //                   ]
    //                 }
    //               }
    //             ]
    //           }
    //         ]
    //       }""".stripMargin,
    //    sc).collect()
    //
    //  enriched.size should be (1) // always one because there's only one json input object
    //  //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
    //
    //  // test the record
    //  val recordMap = enriched.head
    //  recordMap.size should be (1)
    //  val reasonOpt = recordMap.get("reasonForReject")
    //  reasonOpt.isEmpty should be (false)
    //  reasonOpt.get should be ("Invalid KEYFIELD: 'AA'")
    //  recordMap.get("AField") should be (Some("AA"))
    //  recordMap.get("BField") should be (Some("B"))
    //  recordMap.get("CField") should be (Some("10"))
    //  recordMap.get("DField") should be (Some("11"))
    //}
    //
    //it("should add nothing to the enriched record if lookup fails and no onFail is specified") {
    //  val enriched: Array[Map[String, String]] = ActionEngine.process(
    //    "./src/test/resources/input-integration-AA.json", // 'AA' in AField
    //    """{
    //         "activityType": "impressions",
    //         "items": [
    //           {
    //             "source": [ "AField" ], 
    //             "actions":[
    //               {
    //                 "actionType":"lookup",
    //                 "config": {
    //                   "lookupFile": "./src/test/resources/testdimension-table-for-lookup.json",
    //                   "lookupField": "KEYFIELD",
    //                   "fieldsToSelect": [
    //                     "EnhField1",
    //                     "EnhField2",
    //                     "EnhField3"
    //                   ]
    //                 }
    //               }
    //             ]
    //           }
    //         ]
    //       }""".stripMargin,
    //    sc).collect()
    //
    //  enriched.size should be (1) // always one because there's only one json input object
    //  //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
    //
    //  // test the record
    //  val recordMap = enriched.head
    //  recordMap.size should be (1)
    //  recordMap.get("reasonForReject") should be (None)
    //  recordMap.get("AField") should be (None)
    //  recordMap.get("BField") should be (None)
    //  recordMap.get("CField") should be (None)
    //  recordMap.get("DField") should be (None)
    //  recordMap.get("EnhField1") should be (None)
    //  recordMap.get("EnhField2") should be (None)
    //  recordMap.get("EnhField3") should be (None)
    //}

    //it("should throw an exception when parsing the reject action inside process()") {
    //
    //  val e = intercept[Exception]{
    //    val enriched: Array[Map[String, String]] = ActionEngine.process(
    //      "./src/test/resources/input-integration-AA.json", // 'AA' in AField
    //      """{
    //           "activityType": "impressions",
    //           "items": [
    //             {
    //               "source": [ "AField" ], 
    //               "actions":[
    //                 {
    //                   "actionType":"lookup",
    //                   "config": {
    //                     "lookupFile": "./src/test/resources/testdimension-table-for-lookup.json",
    //                     "lookupField": "KEYFIELD",
    //                     "fieldsToSelect": [
    //                       "EnhField1",
    //                       "EnhField2",
    //                       "EnhField3"
    //                     ]
    //                   }
    //                 },
    //                 {
    //                   "actionType": "reject",
    //                 }
    //               ]
    //             }
    //           ]
    //         }""".stripMargin,
    //      sc).collect()
    //  }    
    //
    //  e.toString should be ("'reject' actions need a config section if they are at the top level of the action list.")
    //}

  }

  describe("getAllLookupActions") {
    it("should create a map with all the lookup actions separated into a list") {

      // create sourceactions list:
      val testLookups = List(
        new Lookup(None, Some("db"), Some("tableA"), "lookupFieldA", "sourceField", List("enrichedField0")),
        new Lookup(None, Some("db"), Some("tableB"), "lookupFieldB", "sourceField", List("enrichedField1")),
        new Lookup(None, Some("db"), Some("tableA"), "lookupFieldA", "sourceField", List("enrichedField1")),
        new Lookup(None, Some("db"), Some("tableA"), "lookupFieldA2", "sourceField", List("enrichedField2")),
        new Lookup(None, Some("db"), Some("tableA"), "lookupFieldA", "sourceField", List("enrichedField3"))
      )
      val sourceActions = List(
        SourceAction(
          source = List("inputField0"), 
          actions = List(
            testLookups(0),
            testLookups(1)
          )
        ),
        SourceAction(
          source = List("inputField1"), 
          actions = List(
            testLookups(2),
            testLookups(3),
            testLookups(4)
          )
        ) 
      )

      val gotLookups: Map[String, List[Lookup]] = ActionEngine.getAllLookupActions(sourceActions)

      gotLookups.size should be (2)
      gotLookups.foreach{ case(tableName, lookupList) =>

        tableName match {
          case "db.tableA" => {
            lookupList.size should be (4)
            lookupList(0) should be (testLookups(0))
            lookupList(1) should be (testLookups(2))
            lookupList(2) should be (testLookups(3))
            lookupList(3) should be (testLookups(4))
          }
          case "db.tableB" => {
            lookupList.size should be (1)
            lookupList.head should be (testLookups(1))
          }
          case _ => fail
        }
      }
    }
  }
}

  
