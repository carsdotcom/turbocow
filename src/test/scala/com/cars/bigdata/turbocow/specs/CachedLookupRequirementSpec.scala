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

  describe("getAllFrom") {

    it("should collect all the things") {

      // add test similar to "should collect the rejection reasons if more than one action calls reject"
      // to ensure that it caches the json file name correctly
      // todo

    }

    it("should collect all the lookups for an item that has multiple requirements") {

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

    it("should combine all the lookups properly for an item that has multiFieldKeys") {

      val clReqsList1 = List(
        CachedLookupRequirement(
          "tableA", 
          Nil,
          List("select1", "select2"),
          None,
          multiFieldKeys = Set( Set("A", "B", "C") )
        )
      )

      val clReqsList2 = List(
        CachedLookupRequirement(
          "tableA",
          Nil,
          List("selectB1", "selectB2"),
          None,
          multiFieldKeys = Set( Set("X", "Y", "Z") )
        )
      ) 

      class ActionWithMultiKeyFields1 extends Action {
        override def getLookupRequirements: List[CachedLookupRequirement] = clReqsList1
        override def perform(
          inputRecord: JValue, 
          currentEnrichedMap: Map[String, String],
          context: ActionContext): 
          PerformResult = PerformResult()
      }
      class ActionWithMultiKeyFields2 extends Action {
        override def getLookupRequirements: List[CachedLookupRequirement] = clReqsList2
        override def perform(
          inputRecord: JValue, 
          currentEnrichedMap: Map[String, String],
          context: ActionContext): 
          PerformResult = PerformResult()
      }

      val itemList = List(
        Item(List(new ActionWithMultiKeyFields1(), new ActionWithMultiKeyFields2()))
      )

      // transform to maps to test easier.  (The order can change)
      val allReqs = CachedLookupRequirement.getAllFrom(itemList)

      allReqs.size should be (1)
      val req = allReqs.head

      req.allNeededFields.sorted should be (
        List("select1", "select2", "selectB1", "selectB2"))

      req.keyFields should be (List.empty[String])

      req.multiFieldKeys should be (Set( Set("A", "B", "C"), Set("X", "Y", "Z") ))
    }

  }

}

 

