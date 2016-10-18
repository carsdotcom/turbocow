package com.cars.bigdata.turbocow

import com.cars.bigdata.turbocow.test.SparkTestContext._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.json4s._

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

  import DataFrameUtil._
  sc.setLogLevel("WARN")

  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  // Tests start
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  describe("setDefaultValues()") {

    it("should set default values for all null values according to schema") {
    
      val schemaWithDefaults = List(
        AvroFieldConfig( StructField("id",  StringType, nullable=false), 
          JString("")),
        AvroFieldConfig( StructField("intfield", IntegerType, nullable=true),
          JInt(1)),
        AvroFieldConfig( StructField("longfield", LongType, nullable=true), 
          JInt(2)),
        AvroFieldConfig( StructField("floatfield", FloatType, nullable=true), 
          JDouble(3.0)),
        AvroFieldConfig( StructField("doublefield", DoubleType, nullable=true), 
          JDouble(4.0)),
        AvroFieldConfig( StructField("booleanfield", BooleanType, nullable=true), 
          JBool(false))
      )
      val stSchema = StructType( schemaWithDefaults.map{ _.structField } )

      val df = sqlCtx.createDataFrame( sc.parallelize(
        List(//             id  int   long   float  double boolean
          Row.fromSeq(List( "0", 7,   1L,   2.1f,    7.8,   true)),
          Row.fromSeq(List("10", 17,  null, 12.1f,  null,  null)),
          Row.fromSeq(List("20", null,21L,   null,  27.8,   true)),
          Row.fromSeq(List("30", 37,  31L,   null,  37.8,   null)))),
        stSchema)

      val defaultsDF = setDefaultValues(df, schemaWithDefaults)
      val rows = defaultsDF.collect

      rows.size should be (4)
      rows.foreach{ row => row.getAs[String]("id") match {
        case "0" => 
          row.getAs[Int]("intfield") should be (7)
          row.getAs[Long]("longfield") should be (1L)
          row.getAs[Float]("floatfield") should be (2.1f)
          row.getAs[Double]("doublefield") should be (7.8)
          row.getAs[Boolean]("booleanfield") should be (true)
        case "10" => 
          row.getAs[Int]("intfield") should be (17)
          row.getAs[Long]("longfield") should be (2L)
          row.getAs[Float]("floatfield") should be (12.1f)
          row.getAs[Double]("doublefield") should be (4.0)
          row.getAs[Boolean]("booleanfield") should be (false)
        case "20" => 
          row.getAs[Int]("intfield") should be (1)
          row.getAs[Long]("longfield") should be (21L)
          row.getAs[Float]("floatfield") should be (3.0f)
          row.getAs[Double]("doublefield") should be (27.8)
          row.getAs[Boolean]("booleanfield") should be (true)
        case "30" => 
          row.getAs[Int]("intfield") should be (37)
          row.getAs[Long]("longfield") should be (31L)
          row.getAs[Float]("floatfield") should be (3.0f)
          row.getAs[Double]("doublefield") should be (37.8)
          row.getAs[Boolean]("booleanfield") should be (false)
        case _ => fail()
      }}
    }
  
    it("should set default values for missing fields according to schema") {
      fail()
    }
  }

}


