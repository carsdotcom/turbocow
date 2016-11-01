package com.cars.bigdata.turbocow

import com.cars.bigdata.turbocow.test.SparkTestContext._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.json4s._
import SQLContextUtil._
import DataFrameUtil._
import RowUtil._

class SQLContextUtilSpec
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

  sc.setLogLevel("WARN")

  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  // Tests start
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  describe("createEmptyDataFrame()") {

    it("should have the schema specified and have no rows") {
      val schema = StructType( Array(
        StructField("StringField", StringType, nullable=true),
        StructField("IntField", IntegerType, nullable=true),
        StructField("LongField", LongType, nullable=false),
        StructField("DoubleField", DoubleType, nullable=true),
        StructField("BooleanField", BooleanType, nullable=false),
        StructField("NullField", NullType, nullable=true)
      ))
      val df = sqlCtx.createEmptyDataFrame(schema)

      // no rows
      df.count() should be (0)
      df.collect.size should be (0)

      // check schema
      val fields = df.schema.fields
      fields.size should be (6)
      fields(0) should be (StructField("StringField", StringType, nullable=true))
      fields(1) should be (StructField("IntField", IntegerType, nullable=true))
      fields(2) should be (StructField("LongField", LongType, nullable=false))
      fields(3) should be (StructField("DoubleField", DoubleType, nullable=true))
      fields(4) should be (StructField("BooleanField", BooleanType, nullable=false))
      fields(5) should be (StructField("NullField", NullType, nullable=true))
    }
  }

}



