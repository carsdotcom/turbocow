package com.cars.bigdata.turbocow

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.scalatest.junit.JUnitRunner
import com.cars.bigdata.turbocow.actions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive._

import scala.io.Source
import test.SparkTestContext._

import scala.util.Try

class HiveTableCacheSpec extends UnitSpec {

  var actionFactory: ActionFactory = null
  val testTable = "testtable"
  val testTable2 = "testtable2"

  // before all tests have run
  override def beforeAll() = {
    super.beforeAll()
  }

  // before each test has run
  override def beforeEach() = {
    super.beforeEach()
    actionFactory = new ActionFactory
  }

  // after each test has run
  override def afterEach() = {
    super.afterEach()
    if (hiveCtx.tableNames.contains(testTable)) hiveCtx.dropTempTable(testTable)
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
  val resDir = resourcesDir

  def createTableCaches(config: String): Map[String, TableCache] = {
    val items = actionFactory.createItems(config)
    HiveTableCache.cacheTables(items, Option(hiveCtx))
  }

  def printAllTableNames(msg: String) {
    val allTableNames = hiveCtx.tableNames()
    println(s"""%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% $msg:  allTableNames = ${allTableNames.mkString(", ")}""")
  }

  describe("HiveTableCache")  // ------------------------------------------------
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
           |            "fromFile": "$resDir/testdimension-3row.json",
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

    it("should cache two tables with different indexes properly") {

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
           |            "fromFile": "$resDir/testdimension-3row.json",
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
           |            "fromFile": "$resDir/testdimension-3row.json",
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
      val rowsA = tableCache.tableMap("A")
      val rowsB = tableCache.tableMap("B")
      rowsA.foreach{ rowA =>

        val index = rowA._1 match {
          case s: String => (s.toInt + 1)
          case _ => fail
        }
        val rowB = rowsB( f"${index}%02d" )

        (rowA._2 == rowB) should be (true)
        (rowA._2 eq rowB) should be (true)
      }
    }

    it("should cache two different tables properly") {

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
           |            "fromFile": "$resDir/testdimension-3row.json",
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
           |              "H"
           |            ],
           |            "fromDBTable": "$testTable2",
           |            "fromFile": "$resDir/testdimension2-3row.json",
           |            "where": "F",
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
      numTables should be (2)
      tcMap.keySet should be (Set(testTable, testTable2))

      {
        val tableCache = tcMap.get(testTable).get match { case h: HiveTableCache => h }
        val numIndexes = tableCache.tableMap.size 
        numIndexes should be (1)

        val allIndices = tableCache.tableMap.keys.toList.sorted
        allIndices should be (List("A"))

        // Test A index:
        {
          val rowsMap = tableCache.tableMap("A")
          println("rowsMap = "+rowsMap.toString)
          rowsMap.size should be (3)
          rowsMap.foreach{ row =>
            row._2.schema.fields.map(_.name).toSeq.sorted should be (Seq("A", "B", "C"))
          }

          {
            val row = rowsMap("01")
            row.getAs[String]("A") should be ("01")
            row.getAs[String]("B") should be ("02")
            row.getAs[String]("C") should be ("03")
          }
          {
            val row = rowsMap("11")
            row.getAs[String]("A") should be ("11")
            row.getAs[String]("B") should be ("12")
            row.getAs[String]("C") should be ("13")
          }
          {
            val row = rowsMap("21")
            row.getAs[String]("A") should be ("21")
            row.getAs[String]("B") should be ("22")
            row.getAs[String]("C") should be ("23")
          }
        }
      }

      // do 2nd test table
      {
        val tableCache = tcMap.get(testTable2).get match { case h: HiveTableCache => h }
        val numIndexes = tableCache.tableMap.size 
        numIndexes should be (1)

        val allIndices = tableCache.tableMap.keys.toList.sorted
        allIndices should be (List("F"))

        // Test A index:
        {
          val rowsMap = tableCache.tableMap("F")
          rowsMap.size should be (3)
          rowsMap.foreach{ row =>
            row._2.schema.fields.map(_.name).toSeq.sorted should be (Seq("F", "H"))
          }

          {
            val row = rowsMap("02")
            row.getAs[String]("F") should be ("02")
            row.getAs[String]("H") should be ("04")
          }
          {
            val row = rowsMap("12")
            row.getAs[String]("F") should be ("12")
            row.getAs[String]("H") should be ("14")
          }
          {
            val row = rowsMap("22")
            row.getAs[String]("F") should be ("22")
            row.getAs[String]("H") should be ("24")
          }
        }
      }
    }
  }

  describe("lookup()") {

    val testConfig = s"""{
      |  "activityType": "impressions",
      |  "items": [
      |    {
      |      "actions":[
      |        {
      |          "actionType":"lookup",
      |          "config": {
      |            "select": [
      |              "String", "Int", "Long", "Float", "Double", "Boolean",
      |               "NullInt", "NonNullInt", "AlwaysNull"
      |            ],
      |            "fromDBTable": "$testTable",
      |            "fromFile": "$resDir/testdimension-different-types.json",
      |            "where": "Row",
      |            "equals": "2"
      |          }
      |        }
      |      ]
      |    }
      |  ]
      |}""".stripMargin

    it("should return a row that is requested in lookup call") {
    
      val tcMap: Map[String, TableCache] = createTableCaches(testConfig)
      val tableCache = tcMap.head._2 match { case h: HiveTableCache => h }
    
      val result : Option[Row] = tableCache.lookup("Row", Some("2"))
      result.nonEmpty should be (true)

      val row = result.get
      row.getAs[String]("String") should be ("string")
      row.getAs[Int]("Int") should be (21)
      row.getAs[Long]("Long") should be (22L)
      row.getAs[Double]("Double") should be (-24.1)

      // Note that Float gets converted to Double; it doesn't return a Float:
      row.getAs[Float]("Float") should be (23.1)

      row.getAs[Boolean]("Boolean") should be (true)

      // Note, we have to use a java Integer to set it to null and compare:
      row.getAs[Int]("NullInt") should be ({val v: Integer = null; v})
      row.getAs[Int]("NonNullInt") should be (50)

      // Shouldn't matter the type, this should return null
      row.getAs[Int]("AlwaysNull") should be ({val v: Integer = null; v})
      row.getAs[String]("AlwaysNull") should be ({val v: String = null; v})
      // (Note, again using java type to do null comparison... Row api is annoying...
      row.getAs[Boolean]("AlwaysNull") should be ({val v: java.lang.Boolean = null; v})
    }

    val testConfig2 = s"""{
      |  "activityType": "impressions",
      |  "items": [
      |   {
      |      "actions":[
      |        {
      |          "actionType":"lookup",
      |          "config": {
      |            "select": [
      |              "gg"
      |            ],
      |            "fromDBTable": "$testTable",
      |            "fromFile": "$resDir/testdimension-different-types-invalid-data.json",
      |            "where": "dc",
      |            "equals": "1"
      |          }
      |        }
      |      ]
      |    }
      |  ]
      |}""".stripMargin

    it("should return a row that is requested in lookup call with invalid table values") {
        val tcMap: Map[String, TableCache] = createTableCaches(testConfig2)
        val tableCache = tcMap.head._2 match {
          case h: HiveTableCache => h
        }

        val result: Any = tableCache.convertToCorrectLookupType("dc", "")
        result should be (None)
    }

    it("should return None if keyValue not found") {
    
      val tcMap: Map[String, TableCache] = createTableCaches(testConfig)
      val tableCache = tcMap.head._2 match { case h: HiveTableCache => h }
    
      val result: Option[Row] = tableCache.lookup("Row", Some("X"))
      result should be (None)
    }
  }

}

