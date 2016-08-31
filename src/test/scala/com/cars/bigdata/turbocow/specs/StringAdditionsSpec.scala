package com.cars.bigdata.turbocow

import java.io.File
import java.nio.file.Files

import com.cars.bigdata.turbocow.FileUtil._
import com.cars.bigdata.turbocow.test.SparkTestContext._
import com.databricks.spark.avro._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.util.{Success, Try}

class StringAdditionsSpec
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
  import utils._

  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  // Tests start
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  describe("ltrim()") {

    it("should only trim left whitespace") {
      "test".ltrim should be ("test")
      " test".ltrim should be ("test")
      "  test".ltrim should be ("test")
      "test  ".ltrim should be ("test  ")
    }
  }

  describe("rtrim()") {

    it("should only trim right whitespace") {
      "test".rtrim should be ("test")
      "test ".rtrim should be ("test")
      "test  ".rtrim should be ("test")
      "  test".rtrim should be ("  test")
    }
  }
}
