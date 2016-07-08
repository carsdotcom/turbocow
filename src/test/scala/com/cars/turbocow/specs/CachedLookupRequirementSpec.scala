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
import org.apache.spark.sql.hive.HiveContext

import scala.util.{Try, Success, Failure}

import java.io.File
import java.nio.file.Files

import SparkTestContext._

class CachedLookupRequirementSpec 
  extends UnitSpec 
  //with MockitoSugar 
{
  val testTable = "testTable"

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
  }

}

 

