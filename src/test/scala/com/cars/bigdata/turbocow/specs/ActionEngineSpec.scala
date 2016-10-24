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

class ActionEngineSpec 
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

  describe("processRecord()") // ------------------------------------------------
  {
    it("should add an extra field that lists all the input fields copied in due to not being processed") {
      val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
        // input record:
        List("""{ 
          "md": { 
            "AField": "A", 
            "BField": "B" 
          }, 
          "activityMap": { 
            "CField": 10, 
            "DField": "", 
            "EField": null 
        }}"""),
        // config:
        """{
          "items": [
            {
              "name": "test",
              "actions": [
                { 

                  "actionType": "add-enriched-field",
                  "config": [ 
                    {
                      "key": "BField",
                      "value": "B Value"
                    },
                    {
                      "key": "CField",
                      "value": "C Value"
                    }
                  ]
                }
              ]
            }
          ]
        }""",
        sc).collect()

      enriched.size should be (1) // always one because there's only one json input object
      enriched.head.size should be (5 + 1) // input records always get copied in, plus the addedInputFieldsMarker
      enriched.head.get("AField") should be (Some("A"))
      enriched.head.get("BField") should be (Some("B Value"))
      enriched.head.get("CField") should be (Some("C Value"))
      enriched.head.get("DField") should be (Some(""))
      enriched.head.get("EField") should be (Some(null)) // i know this is weird

      val inputFields = enriched.head.get(ActionEngine.addedInputFieldsMarker)
      inputFields.nonEmpty should be (true)
      val inputFieldsList = inputFields.get.split(",").toSeq
      inputFieldsList.size should be (3)

      // only the not-processed fields should be marked as 'added input fields'
      inputFieldsList.contains("AField") should be (true)
      inputFieldsList.contains("DField") should be (true)
      inputFieldsList.contains("EField") should be (true)
    }

    it("should copy in all fields from the input record if no actions specified") {
      val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
        // input record:
        List("""{ 
          "md": { 
            "AField": "A", 
            "BField": "B" 
          }, 
          "activityMap": { 
            "CField": 10, 
            "DField": "", 
            "EField": null 
        }}"""),
        // config:
        """{
          "items": []
        }""",
        sc).collect()

      enriched.size should be (1) // always one because there's only one json input object
      enriched.head.size should be (5) // input records always get copied in
      enriched.head.get("AField") should be (Some("A"))
      enriched.head.get("BField") should be (Some("B"))
      enriched.head.get("CField") should be (Some("10"))
      enriched.head.get("DField") should be (Some(""))
      enriched.head.get("EField") should be (Some(null)) // i know this is weird
    }

    it("should copy in all fields from the input record even if an empty config is specified") {
      val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
        // input record:
        List("""{
          "md": {
            "AField": "A",
            "BField": "B"
          },
          "activityMap": {
            "CField": 10,
            "DField": "",
            "EField": null
        }}"""),
        // config:
        """{}""",
        sc).collect()

      enriched.size should be (1) // always one because there's only one json input object
      enriched.head.size should be (5) // input records always get copied in
      enriched.head.get("AField") should be (Some("A"))
      enriched.head.get("BField") should be (Some("B"))
      enriched.head.get("CField") should be (Some("10"))
      enriched.head.get("DField") should be (Some(""))
      enriched.head.get("EField") should be (Some(null)) // i know this is weird
    }

    it("""should auto-copy fields from input to enriched only if they don't already
          exist in enriched""") {
    
      val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
        // inputJson
        Seq(s"""{"A": "AVAL", "B": "BVAL", "C": "CVAL"}"""),
        // config
        s"""{
          "activityType": "impressions",
    
          "items": [
            {
              "name": "test",
              "actions":[
                {
                  "actionType":"add-enriched-fields",
                  "config": [{
                      "key": "C",
                      "value": "ENRICHED_VALUE"
                  }]
                }
              ]
            }
          ]
        }""",
        sc
      ).collect()
    
      enriched.size should be (1)
      enriched.head.size should be (3)
      enriched.head("A") should be ("AVAL") // from input
      enriched.head("B") should be ("BVAL") // from input
      enriched.head("C") should be ("ENRICHED_VALUE") // from enriched
    }
  }

  describe("copy action") {
    it("should successfully process 'copy' with single config element") {

      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration.json"),
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
      enriched.head.size should be (5 + 1)
      enriched.head.get("AFieldEnriched") should be (Some("A"))
    }

    it("should successfully process 'copy' with two config elements") {

      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration.json"),
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
      enriched.head.size should be (2 + 5)
      enriched.head.get("AFieldEnriched") should be (Some("A"))
      enriched.head.get("BFieldEnriched") should be (Some("B"))
    }

    it("should successfully 'copy' over blank values from the input record, if specified") {

      // Note: EField value is "" in the input record
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration.json"),
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
      enriched.head.size should be (2 + 5)
      enriched.head.get("AFieldEnriched") should be (Some("A"))
      enriched.head.get("EFieldEnriched") should be (Some(""))
    }

    it("should throw exception on construct if 'copy' action has a null inputSource") {

      intercept[Exception] {
        ActionEngine.processDir(
          new URI("./src/test/resources/input-integration.json"),
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
        ActionEngine.processDir(
          new URI("./src/test/resources/input-integration.json"),
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
        ActionEngine.processDir(
          new URI("./src/test/resources/input-integration.json"),
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
        ActionEngine.processDir(
          new URI("./src/test/resources/input-integration.json"),
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
        ActionEngine.processDir(
          new URI("./src/test/resources/input-integration.json"),
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
  
  describe("custom actions") {

    it("should successfully process a custom action") {
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration.json"),
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
      enriched.head.size should be (2 + 5)
      enriched.head("customA") should be ("AAA")
      enriched.head("customB") should be ("BBB")
    }
  }

  describe("misc ") {
    
    //it("should successfully process a different custom action") {
    //  val enriched: List[Map[String, String]] = ActionEngine.processDir(
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
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration.json"),
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

  describe("getAllFrom") {
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

      val reqs: List[CachedLookupRequirement] = CachedLookupRequirement.getAllFrom(items)

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

  describe("processJsonStrings()") {

    it("""should run the exceptionHandlingList actions if an unhandled 
         exception is thrown""") {

      val t = Try {
        ActionEngine.processJsonStrings(
          // inputJson
          Seq(s"""{"A": "AVAL", "B": "BVAL"}"""),
          // config
          s"""{
            "activityType": "impressions",

            "global": {
              "exceptionHandlingList": [{
                "actionType": "add-enriched-field",
                "config": [{
                  "key": "result", 
                  "value": "success"
                }]
              }]
            },

            "items": [
              {
                "name": "test",
                "actions":[{
                  "actionType":"mock-action",
                  "config": { "shouldThrow": true }
                }]
              }
            ]
          }""",
          sc,
          Option(hiveCtx),
          new ActionFactory(new CustomActionCreator) 
        )
      }

      if (t.isFailure) println("Exception: "+t.get)
      t.isSuccess should be (true)
      val collected = t.get.collect
      collected.size should be (1) // only 1 record
      val enrichedMap = collected.head

      enrichedMap should be (Map("result"->"success"))
    }

    it("""should detect unhandled exceptions and add the stack trace to the 
          reject reason field""") {

      val t = Try {
        ActionEngine.processJsonStrings(
          // inputJson
          Seq(s"""{"A": "AVAL", "B": "BVAL"}"""),
          // config
          s"""{
            "activityType": "impressions",

            "global": {
              "exceptionHandlingList": [
                {
                  "actionType": "reject",
                  "config": {
                    "reasonFrom": "unhandled-exception",
                    "stopProcessingActionList": false
                  }
                },
                {
                  "actionType":"add-rejection-reason",
                  "config": {
                    "field": "reasonForReject"
                  }
                },      
                {
                  "actionType":"check",
                  "config": {
                    "field": "reasonForReject",
                    "fieldSource": "enriched",
                    "op": "empty",
                    "onPass": [{
                      "actionType": "add-enriched-field",
                      "config": [{
                        "key": "accepted", 
                        "value": "true"
                      }]
                    }],
                    "onFail": [{
                      "actionType": "add-enriched-field",
                      "config": [{
                        "key": "accepted", 
                        "value": "false"
                      }]
                    }]
                  }
                }
              ]
            },

            "items": [
              {
                "name": "test",
                "actions":[
                  {
                    "actionType":"add-enriched-fields",
                    "config": [{
                        "key": "E",
                        "value": "EVAL"
                    }]
                  },
                  {
                    "actionType":"mock-action",
                    "config": { "shouldThrow": true }
                  }
                ]
              }
            ]
          }""",
          sc,
          Option(hiveCtx),
          new ActionFactory(new CustomActionCreator) 
        )
      }

      if (t.isFailure) println("Exception: "+t.get)
      t.isSuccess should be (true)
      val collected = t.get.collect
      collected.size should be (1) // only 1 record
      val enrichedMap = collected.head

      enrichedMap.keySet should be (Set("A", "B", "accepted", "reasonForReject"))
      val expectedStart = "Unhandled Exception:  MockAction:  throwing exception"
      enrichedMap("reasonForReject").substring(0, expectedStart.size) should be (expectedStart)
      // there should be more stuff after this... (the stack trace)
      enrichedMap("reasonForReject").size should be > (expectedStart.size)

      // check the start of the stack trace...
      val nextLine = "\n    com.cars.bigdata.turbocow.actions.MockAction.perform("
      enrichedMap("reasonForReject").substring(expectedStart.size, expectedStart.size + nextLine.size) should
        be (nextLine)

      // this field was configured too
      enrichedMap("accepted") should be ("false")

      // should have added the entire record too:
      enrichedMap("A") should be ("AVAL")
      enrichedMap("B") should be ("BVAL")
    }

    it("""should not throw if the exceptionHandling stanza is not configured""") {

      val t = Try {
        ActionEngine.processJsonStrings(
          // inputJson
          Seq(s"""{"A": "AVAL", "B": "BVAL"}"""),
          // config
          s"""{
            "activityType": "impressions",

            "items": [
              {
                "name": "test",
                "actions":[
                  {
                    "actionType":"add-enriched-fields",
                    "config": [{
                        "key": "E",
                        "value": "EVAL"
                    }]
                  },
                  {
                    "actionType":"mock-action",
                    "config": { "shouldThrow": true }
                  }
                ]
              }
            ]
          }""",
          sc,
          Option(hiveCtx),
          new ActionFactory(new CustomActionCreator) 
        )
      }

      t.isSuccess should be (true)
      val collected = t.get.collect
      collected.size should be (0)
    }

  }
  
}

 
