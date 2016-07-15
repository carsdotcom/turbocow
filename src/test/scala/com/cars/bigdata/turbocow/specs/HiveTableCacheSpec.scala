package com.cars.bigdata.turbocow

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.scalatest.junit.JUnitRunner
import com.cars.bigdata.turbocow.actions._
import org.apache.spark.sql.hive._

import scala.io.Source
import test.SparkTestContext._
import scala.util.Try

class HiveTableCacheSpec extends UnitSpec {

  var actionFactory: ActionFactory = null
  val testTable = "testtable"

  // before all tests have run
  override def beforeAll() = {
    super.beforeAll()
  }

  // before each test has run
  override def beforeEach() = {
    super.beforeEach()
    actionFactory = new ActionFactory
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

  val resourcesDir = "./src/test/resources/"
  val resDir = resourcesDir

  def createTableCaches(config: String): Map[String, TableCache] = {
    val items = actionFactory.createItems(config)
    HiveTableCache.cacheTables(items, Option(hiveCtx))
  }

  def printAllTableNames(msg: String) {
    val allTableNames = hiveCtx.tableNames()
    println(s"""%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% $msg:  allTableNames = ${allTableNames.mkString(", ")}""")
  }

  describe("cacheTables")  // ------------------------------------------------
  {
    it("should cache one table properly") {

      val config = 
        s"""{
           |  "activityType": "impressions",
           |  "items": [
           |    {
           |      "actions":[
           |        {
           |          "actionType":"lookup",
           |          "config": {
           |            "select": [
           |              "B",
           |              "C"
           |            ],
           |            "fromDBTable": "$testTable",
           |            "fromFile": "$resDir/testdimension-2row.json",
           |            "where": "A",
           |            "equals": "11"
           |          }
           |        }
           |      ]
           |    }
           |  ]
           |}""".stripMargin


      printAllTableNames("BEFORE creating cache 1")

      val tcMap: Map[String, TableCache] = createTableCaches(config)

      printAllTableNames("AFTER creating cache 1")

      val numTables = tcMap.size
      numTables should be (1)
      val table = tcMap.head
      val tableName = table._1
      tableName should be (testTable)

      val tableCache = table._2 match { case h: HiveTableCache => h }
      val numIndexes = tableCache.tableMap.size 
      numIndexes should be (1)

      val indexField = tableCache.tableMap.head._1
      indexField should be ("A")

      val rowsMap = tableCache.tableMap.head._2
      rowsMap.size should be (3)

      {
        val row = rowsMap("01")
        row.getAs[String]("A") should be ("01")
        row.getAs[String]("B") should be ("02")
        row.getAs[String]("C") should be ("03")
        row.size should be (3)
      }
      {
        val row = rowsMap("11")
        row.getAs[String]("A") should be ("11")
        row.getAs[String]("B") should be ("12")
        row.getAs[String]("C") should be ("13")
        row.size should be (3)
      }
      {
        val row = rowsMap("21")
        row.getAs[String]("A") should be ("21")
        row.getAs[String]("B") should be ("22")
        row.getAs[String]("C") should be ("23")
        row.size should be (3)
      }
    }

    it("should cache two tables properly") {

      val config = 
        s"""{
           |  "activityType": "impressions",
           |  "items": [
           |    {
           |      "actions":[
           |        {
           |          "actionType":"lookup",
           |          "config": {
           |            "select": [
           |              "B",
           |              "C"
           |            ],
           |            "fromDBTable": "$testTable",
           |            "fromFile": "$resDir/testdimension-2row.json",
           |            "where": "A",
           |            "equals": "11"
           |          }
           |        }
           |      ]
           |    },
           |    {
           |      "actions":[
           |        {
           |          "actionType":"lookup",
           |          "config": {
           |            "select": [
           |              "D"
           |            ],
           |            "fromDBTable": "$testTable",
           |            "fromFile": "$resDir/testdimension-2row.json",
           |            "where": "B",
           |            "equals": "22"
           |          }
           |        }
           |      ]
           |    }
           |  ]
           |}""".stripMargin

      printAllTableNames("BEFORE creating cache 2")

      val tcMap: Map[String, TableCache] = createTableCaches(config)

      printAllTableNames("AFTER creating cache 2")

      val numTables = tcMap.size
      numTables should be (1)
      val table = tcMap.head
      val tableName = table._1
      tableName should be (testTable)

      val tableCache = table._2 match { case h: HiveTableCache => h }
      val numIndexes = tableCache.tableMap.size 
      numIndexes should be (2)

      val allIndices = tableCache.tableMap.keys.toList.sorted
      allIndices should be (List("A", "B"))

      // Test A index:
      {
        val rowsMap = tableCache.tableMap("A")
        rowsMap.size should be (3)
        ;{
          val row = rowsMap("01")
          row.getAs[String]("A") should be ("01")
          row.getAs[String]("B") should be ("02")
          row.getAs[String]("C") should be ("03")
          row.getAs[String]("D") should be ("04")
          row.size should be (4)
        }
        {
          val row = rowsMap("11")
          row.getAs[String]("A") should be ("11")
          row.getAs[String]("B") should be ("12")
          row.getAs[String]("C") should be ("13")
          row.getAs[String]("D") should be ("14")
          row.size should be (4)
        }
        {
          val row = rowsMap("21")
          row.getAs[String]("A") should be ("21")
          row.getAs[String]("B") should be ("22")
          row.getAs[String]("C") should be ("23")
          row.getAs[String]("D") should be ("24")
          row.size should be (4)
        }
      }

      // Test B index:
      {
        val rowsMap = tableCache.tableMap("B")
        rowsMap.size should be (3)
        ;{
          val row = rowsMap("02")
          row.getAs[String]("A") should be ("01")
          row.getAs[String]("B") should be ("02")
          row.getAs[String]("C") should be ("03")
          row.getAs[String]("D") should be ("04")
          row.size should be (4)
        }
        {
          val row = rowsMap("12")
          row.getAs[String]("A") should be ("11")
          row.getAs[String]("B") should be ("12")
          row.getAs[String]("C") should be ("13")
          row.getAs[String]("D") should be ("14")
          row.size should be (4)
        }
        {
          val row = rowsMap("22")
          row.getAs[String]("A") should be ("21")
          row.getAs[String]("B") should be ("22")
          row.getAs[String]("C") should be ("23")
          row.getAs[String]("D") should be ("24")
          row.size should be (4)
        }
      }

      // Ensure that the Row objects are the same for each index-map:
      //TODO
    }
     
  }

}

