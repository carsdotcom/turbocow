package com.cars.bigdata.turbocow

import org.scalatest.junit.JUnitRunner
import com.cars.bigdata.turbocow.actions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

import scala.io.Source
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

import com.databricks.spark.avro._
import org.apache.spark.sql.hive.HiveContext

import scala.util.{Try, Success, Failure}

import org.json4s._

import java.io.File
import java.nio.file.Files

import test.SparkTestContext._

class CachedLookupRequirementSpec 
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
  }

  // after each test has run
  override def afterEach() = {
    //if (hiveCtx.tableNames.contains(testTable)) hiveCtx.dropTempTable(testTable)
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

  val clReqsList = List(
    CachedLookupRequirement(
      "tableA", 
      List("key1", "key2"),
      List("select1", "select2")
    ),
    CachedLookupRequirement(
      "tableB", 
      List("keyB1", "keyB2"),
      List("selectB1", "selectB2")
    )
  ) 

  describe("getAllFrom") {

    it("should collect all the things") {

      // add test similar to "should collect the rejection reasons if more than one action calls reject"
      // to ensure that it caches the json file name correctly
      // todo

    }

    it("should collect all the lookups for an item that has multiple requirements") {

      class ActionWithManyLookupRequirements extends Action {

        override def getLookupRequirements: List[CachedLookupRequirement] = clReqsList

        override def perform(
          inputRecord: JValue, 
          currentEnrichedMap: Map[String, String],
          context: ActionContext): 
          PerformResult = PerformResult()
      }

      val itemList = List(
        Item(List(new ActionWithManyLookupRequirements()))
      )

      // transform to maps to test easier.  (The order can change)
      val gotReqsMap = CachedLookupRequirement.getAllFrom(itemList).map{ req =>
        (req.dbTableName, req) 
      }.toMap

      val shouldMap = clReqsList.map{ req => (req.dbTableName, req) }.toMap

      gotReqsMap.size should be (2)
      gotReqsMap.get("tableA") should be (shouldMap.get("tableA"))
      gotReqsMap.get("tableB") should be (shouldMap.get("tableB"))
    }
  }

}

 

