package com.cars.bigdata.turbocow

import java.io.File
import java.nio.file.Files

import org.json4s._
import org.json4s.jackson.JsonMethods._

import com.cars.bigdata.turbocow.utils.FileUtil._
import com.cars.bigdata.turbocow.test.SparkTestContext._
import com.databricks.spark.avro._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.io.Source
import scala.util.{Success, Try}

class DataFrameUtilSpec
  extends UnitSpec 
{
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
    super.afterEach()
  }

  // after all tests have run
  override def afterAll() = {
    super.afterAll()
  }

  import AvroOutputWriter._

  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  // Tests start
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  describe("convertEnrichedRDDToDataFrame()") {

    it("should copy over all fields when enrichedRDD matches schema exactly") {
      fail()
    }

    it("should set any missing enrichedRDD fields to null") {
      fail()
    }

    it("should NOT add any extra enrichedRDD fields to the output dataframe") {
      fail()
    }

    it("should output empty DataFrame if input RDD is empty") {
      fail()
    }

    it("should throw if schema is null") {
      fail()
    }

    it("should throw if schema is an empty List") {
      fail()
    }

  }

  describe("setDefaultValues()") {
    it("should set default values for all null values according to schema") {
      fail()
    }

    it("should set default values for missing fields according to schema") {
      fail()
    }
  }
    
}


