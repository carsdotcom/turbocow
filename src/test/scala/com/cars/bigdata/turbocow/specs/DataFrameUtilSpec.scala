package com.cars.bigdata.turbocow

import java.io.File
import java.nio.file.Files

import org.json4s._
import org.json4s.jackson.JsonMethods._

import com.cars.bigdata.turbocow.utils.FileUtil._
import com.cars.bigdata.turbocow.test.SparkTestContext._
import org.apache.spark.sql.types._
import com.databricks.spark.avro._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.io.Source
import scala.util.{Success, Try}

import DataFrameUtil._
import AvroOutputWriter._

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


}


