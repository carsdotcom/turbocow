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

  describe("simple copy") // ------------------------------------------------
  {
    it("should successfully process one field") {
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration.json"),
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
    
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration.json"),
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
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration.json"),
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
        ActionEngine.processDir(
          new URI("./src/test/resources/input-integration.json"),
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
        ActionEngine.processDir(
          new URI("./src/test/resources/input-integration.json"),
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
        ActionEngine.processDir(
          new URI("./src/test/resources/input-integration.json"),
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
        ActionEngine.processDir(
          new URI("./src/test/resources/input-integration.json"),
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
      enriched.head.size should be (1)
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
      enriched.head.size should be (2)
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
      enriched.head.size should be (2)
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
      enriched.head.size should be (2)
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

    it("should be able to enrich from the scratchpad when a test value is set") {

      val scratchPad: ScratchPad = new ScratchPad()
      scratchPad.set("test","test123")
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration.json"),
        """{
          |  "activityType":"impressions",
          |  "items":[
          |    {
          |      "actions":[
          |        {
          |          "actionType":"add-scratch-to-enriched"
          |        }
          |      ]
          |    }
          |  ]
          |}""".stripMargin, sc,
        None,
        new ActionFactory(new CustomActionCreator),
        scratchPad).collect()

      enriched.size should be (1) // always one because there's only one json input object
      enriched.head("test") should be ("test123")

    }

    it("should return empty map in enriched when trying to enrich on an empty scratchpad") {

      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration.json"),
        """{
          |  "activityType":"impressions",
          |  "items":[
          |    {
          |      "actions":[
          |        {
          |          "actionType":"add-scratch-to-enriched"
          |        }
          |      ]
          |    }
          |  ]
          |}""".stripMargin, sc,
        None,
        new ActionFactory(new CustomActionCreator)).collect()

      enriched.size should be (1) // always one because there's only one json input object
      enriched.head.size should be (0)
    }
  }

  describe("AvroOutputWriter") {
    it("should only output the fields in the schema regardless of what is in the input RDD") {
      val enriched: RDD[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration.json"),
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

 
