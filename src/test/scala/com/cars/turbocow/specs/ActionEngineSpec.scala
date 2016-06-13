package com.cars.turbocow

import org.scalatest.junit.JUnitRunner
import com.cars.turbocow._
import com.cars.turbocow.actions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

import scala.io.Source

// Fix for Scalatest on Gradle:  (from http://stackoverflow.com/questions/18823855/cant-run-scalatest-with-gradle)
// Alternately, try using https://github.com/maiflai/gradle-scalatest
//@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class TurboCowSpec extends UnitSpec {

  // initialise spark context
  //val conf = new SparkConf().setAppName("TurboCowSpec").setMaster("local[1]")
  val conf = new SparkConf().setAppName("TurboCowSpec").setMaster("local[2]")
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

  describe("enrich()")  // ------------------------------------------------
  {
    it("should successfully process simple-copy") {
    
      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration.json",
        fileToString("./src/test/resources/testconfig-integration-simplecopy.json"),
        sc).collect()
  
      enriched.size should be (1) // always one because there's only one json input object
      //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
      enriched.head("AField") should be ("A")
      enriched.head("CField") should be ("10")
    }

    it("should throw exception if 'copy' action has multiple sources") {

      try {
        val enriched: Array[Map[String, String]] = ActionEngine.process(
          "./src/test/resources/input-integration.json",
          """{
                    |   "activityType":"impressions",
                    |   "items":[
                    |      {
                    |         "source":[
                    |            "AField",
                    |            "CField"
                    |         ],
                    |         "actions":[
                    |            {
                    |               "actionType":"copy",
                    |               "config":{
                    |                  "newName":"time_stamp"
                    |               }
                    |            }
                    |         ]
                    |      }
                    |   ]
                    |}""".stripMargin,
          sc).collect()

        fail()
      }
      catch{
        case e: Throwable =>
      }
    }

    it("should successfully process 'copy' with config segment with single source ") {

      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration.json",
        """{
                   |   "activityType":"impressions",
                   |   "items":[
                   |      {
                   |         "source":[
                   |            "AField"
                   |         ],
                   |         "actions":[
                   |            {
                   |               "actionType":"copy",
                   |               "config":{
                   |                  "newName":"time_stamp"
                   |               }
                   |            }
                   |         ]
                   |      }
                   |   ]
                   |}""".stripMargin,
        sc).collect()

      enriched.size should be (1) // always one because there's only one json input object
      //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
      enriched.head("time_stamp") should be ("A")
    }

    it("should throw exception if 'copy' action has a null newName") {

      try {
        val enriched: Array[Map[String, String]] = ActionEngine.process(
          "./src/test/resources/input-integration.json",
          """{
                     |   "activityType":"impressions",
                     |   "items":[
                     |      {
                     |         "source":[
                     |            "AField"
                     |         ],
                     |         "actions":[
                     |            {
                     |               "actionType":"copy",
                     |               "config":{
                     |                  "newName":null
                     |               }
                     |            }
                     |         ]
                     |      }
                     |   ]
                     |}""".stripMargin,
          sc).collect()

        fail()
      }
      catch{
        case e: Throwable =>
      }
    }

    it(" should throw an exception if 'copy' action does not have config object") {

      try {
        val enriched: Array[Map[String, String]] = ActionEngine.process(
          "./src/test/resources/input-integration.json",
          """{
                     |  "activityType" : "impressions",
                     |  "items" : [
                     |    {
                     |        "source" : [ "AField" ],
                     |        "actions" : [
                     |        {
                     |            "actionType" : "copy"
                     |        }
                     |      ]
                     |    }
                     |  ]
                     |}""".stripMargin,
          sc).collect()

        fail()
        }
      catch{
        case e: Throwable =>
      }
    }

    /** Helper test function
      */
    def testReplaceNullWith(value: Int) = {
      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration-replacenullwith.json",
        fileToString(s"./src/test/resources/testconfig-integration-replacenullwith${value.toString}.json"),
        sc).collect()

      enriched.size should be (1) // always one because there's only one json input object
      //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
      enriched.head("AField") should be ("A")
      enriched.head("CField") should be (value.toString) // this one was null
      enriched.head("DField") should be (value.toString) // this one was missing
    }

    it("should successfully process replace-null-with-0") {
      testReplaceNullWith(0)
    }

    // make sure any value will work
    it("should successfully process replace-null-with-999") {
      testReplaceNullWith(999)
    }

    it("should successfully process a lookup action") {
      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration.json",
        fileToString("./src/test/resources/testconfig-integration-lookup.json"),
        sc).collect()
  
      enriched.size should be (1) // always one because there's only one json input object
      //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
      enriched.head("EnhField1") should be ("1")
      enriched.head("EnhField2") should be ("2")
      enriched.head("EnhField3") should be ("3")
    }

    it("should successfully process a custom action") {
      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration.json",
        fileToString("./src/test/resources/testconfig-integration-custom.json"),
        sc, 
        None,
        new ActionFactory(new CustomActionCreator) ).collect()
    
      enriched.size should be (1) // always one because there's only one json input object
      //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
      enriched.head("EnhField1") should be ("1")
      enriched.head("EnhField2") should be ("2")
      enriched.head("EnhField3") should be ("3")
      enriched.head("customA") should be ("AAA")
      enriched.head("customB") should be ("BBB")
    }
    
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

  describe("getAllLookupActions") {
    it("should create a map with all the lookup actions separated into a list") {

      // create sourceactions list:
      val testLookups = List(
        new Lookup(None, Some("db"), Some("tableA"), "lookupFieldA", List("enrichedField0")),
        new Lookup(None, Some("db"), Some("tableB"), "lookupFieldB", List("enrichedField1")),
        new Lookup(None, Some("db"), Some("tableA"), "lookupFieldA", List("enrichedField1")),
        new Lookup(None, Some("db"), Some("tableA"), "lookupFieldA2", List("enrichedField2")),
        new Lookup(None, Some("db"), Some("tableA"), "lookupFieldA", List("enrichedField3"))
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

  
