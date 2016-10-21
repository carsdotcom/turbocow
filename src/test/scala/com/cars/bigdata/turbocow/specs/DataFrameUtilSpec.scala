package com.cars.bigdata.turbocow

import com.cars.bigdata.turbocow.test.SparkTestContext._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.json4s._
import RowUtil._

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

  describe("addColumnWithDefaultValue()") {
    val schema = List(
      AvroFieldConfig( StructField("id",  StringType, nullable=false), 
        JString(""))
    )
    val stSchema = StructType( schema.map{ _.structField }.toArray )
    val startDF = sqlCtx.createDataFrame( sc.parallelize(
      List(//             id 
        Row.fromSeq(List( "0")),
        Row.fromSeq(List("10")))),
      stSchema)

    it("should add a string column with string default") {
      val df = addColumnWithDefaultValue(
        startDF, 
        AvroFieldConfig(StructField("newfield", StringType), JString("STRDEF"))) 

      // check schema
      df.schema.size should be (2)
      df.schema(0).name should be ("id")
      df.schema(1).name should be ("newfield")
      df.schema(1).dataType should be (StringType)
      
      // check data
      val rows = df.collect()
      rows.size should be (2)
      rows.foreach{ row =>
        row.getAs[String]("newfield") should be ("STRDEF")
      }
    }

    it("should add a string column with null default") {
      val df = addColumnWithDefaultValue(
        startDF, 
        AvroFieldConfig(StructField("newfield", StringType), JNull)) 

      // check schema
      df.schema.size should be (2)
      df.schema(0).name should be ("id")
      df.schema(1).name should be ("newfield")
      df.schema(1).dataType should be (StringType)
      
      // check data
      val rows = df.collect()
      rows.size should be (2)
      rows.foreach{ row =>
        row.size should be (2)
        row.fieldIsNull("id") should be (false)
        row.fieldIsNull("newfield") should be (true)
      }
    }

    it("should add a int column with int default") {
      val df = addColumnWithDefaultValue(
        startDF,
        AvroFieldConfig(StructField("newfield", IntegerType), JInt(101)))

      // check schema
      df.schema.size should be (2)
      df.schema(0).name should be ("id")
      df.schema(1).name should be ("newfield")
      df.schema(1).dataType should be (IntegerType)

      // check data
      val rows = df.collect()
      rows.size should be (2)
      rows.foreach{ row =>
        row.getAs[Int]("newfield") should be (101)
      }

    }
    it("should add a int column with null default") {
      val df = addColumnWithDefaultValue(
        startDF, 
        AvroFieldConfig(StructField("newfield", IntegerType), JNull)) 

      // check schema
      df.schema.size should be (2)
      df.schema(0).name should be ("id")
      df.schema(1).name should be ("newfield")
      df.schema(1).dataType should be (IntegerType)
      
      // check data
      val rows = df.collect()
      rows.size should be (2)
      rows.foreach{ row =>
        row.size should be (2)
        row.fieldIsNull("id") should be (false)
        row.fieldIsNull("newfield") should be (true)
      }
    }

    // skipping long...

    // skipping float...

    it("should add a double column with double default") {
      val df = addColumnWithDefaultValue(
        startDF,
        AvroFieldConfig(StructField("newfield", DoubleType), JDouble(10.1)))

      // check schema
      df.schema.size should be (2)
      df.schema(0).name should be ("id")
      df.schema(1).name should be ("newfield")
      df.schema(1).dataType should be (DoubleType)

      // check data
      val rows = df.collect()
      rows.size should be (2)
      rows.foreach{ row =>
        row.getAs[Double]("newfield") should be (10.1)
      }

    }
    it("should add a double column with null default") {
      val df = addColumnWithDefaultValue(
        startDF,
        AvroFieldConfig(StructField("newfield", DoubleType), JNull))

      // check schema
      df.schema.size should be (2)
      df.schema(0).name should be ("id")
      df.schema(1).name should be ("newfield")
      df.schema(1).dataType should be (DoubleType)

      // check data
      val rows = df.collect()
      rows.size should be (2)
      rows.foreach{ row =>
        row.size should be (2)
        row.fieldIsNull("id") should be (false)
        row.fieldIsNull("newfield") should be (true)
      }
    }

    it("should add a boolean column with boolean default") {
      val df = addColumnWithDefaultValue(
        startDF,
        AvroFieldConfig(StructField("newfield", BooleanType), JBool(true)))

      // check schema
      df.schema.size should be (2)
      df.schema(0).name should be ("id")
      df.schema(1).name should be ("newfield")
      df.schema(1).dataType should be (BooleanType)

      // check data
      val rows = df.collect()
      rows.size should be (2)
      rows.foreach{ row =>
        row.getAs[Boolean]("newfield") should be (true)
      }

    }
    it("should add a bool column with null default") {
      val df = addColumnWithDefaultValue(
        startDF, 
        AvroFieldConfig(StructField("newfield", BooleanType), JNull)) 

      // check schema
      df.schema.size should be (2)
      df.schema(0).name should be ("id")
      df.schema(1).name should be ("newfield")
      df.schema(1).dataType should be (BooleanType)
      
      // check data
      val rows = df.collect()
      rows.size should be (2)
      rows.foreach{ row =>
        row.size should be (2)
        row.fieldIsNull("id") should be (false)
        row.fieldIsNull("newfield") should be (true)
      }
    }
  }

  describe("setDefaultValues()") {

    it("should set default values for all null values according to schema") {
    
      val schemaWithDefaults = List(
        AvroFieldConfig( StructField("id",  StringType, nullable=false), 
          JString("")),
        AvroFieldConfig( StructField("intfield", IntegerType, nullable=true),
          JInt(1)),
        AvroFieldConfig( StructField("longfield", LongType, nullable=true), 
          JNull),
        //AvroFieldConfig( StructField("floatfield", FloatType, nullable=true), 
        //  JDouble(3.0)),
        AvroFieldConfig( StructField("doublefield", DoubleType, nullable=true), 
          JDouble(4.0)),
        AvroFieldConfig( StructField("booleanfield", BooleanType, nullable=true),
          JBool(false)),
        AvroFieldConfig( StructField("booleanfield2", BooleanType, nullable=true),
          JNull)
      )
      val stSchema = StructType( schemaWithDefaults.map{ _.structField }.toArray )

      //val df = sqlCtx.createDataFrame( sc.parallelize(
      //  List(//             id  int   long  float  double boolean
      //    Row.fromSeq(List( "0", 7,   1L,   2.1f,    7.8,   true)),
      //    Row.fromSeq(List("10", 17,  null, 12.1f,  null,  null)),
      //    Row.fromSeq(List("20", null,21L,   null,  27.8,   false)),
      //    Row.fromSeq(List("30", 37,  31L,   null,  37.8,   null)))),
      //  stSchema)
      val df = sqlCtx.createDataFrame( sc.parallelize(
        List(//             id  int   long  double boolean, boolean2
          Row.fromSeq(List( "0", 7,   1L,     7.8, true,    true)),
          Row.fromSeq(List("10", 17,  null,  null, null,    null)),
          Row.fromSeq(List("20", null,21L,   27.8, false,   false)),
          Row.fromSeq(List("30", 37,  31L,   37.8, null,    null)))),
        stSchema)

      val defaultsDF = setDefaultValues(df, schemaWithDefaults)
      val rows = defaultsDF.collect

      rows.size should be (4)
      rows.foreach{ row => row.getAs[String]("id") match {
        case "0" => 
          row.getAs[Int]("intfield") should be (7)
          row.getAs[Long]("longfield") should be (1L)
          //row.getAs[Float]("floatfield") should be (2.1f)
          row.getAs[Double]("doublefield") should be (7.8)
          row.getAs[Boolean]("booleanfield") should be (true)
          row.getAs[Boolean]("booleanfield2") should be (true)
        case "10" => 
          row.getAs[Int]("intfield") should be (17)
          row.fieldIsNull("longfield") should be (true)
          //row.getAs[Float]("floatfield") should be (12.1f)
          row.getAs[Double]("doublefield") should be (4.0)
          row.getAs[Boolean]("booleanfield") should be (false)
          row.fieldIsNull("booleanfield2") should be (true)
        case "20" => 
          row.getAs[Int]("intfield") should be (1)
          row.getAs[Long]("longfield") should be (21L)
          //row.getAs[Float]("floatfield") should be (3.0f)
          row.getAs[Double]("doublefield") should be (27.8)
          row.getAs[Boolean]("booleanfield") should be (false)
          row.getAs[Boolean]("booleanfield2") should be (false)
        case "30" => 
          row.getAs[Int]("intfield") should be (37)
          row.getAs[Long]("longfield") should be (31L)
          //row.getAs[Float]("floatfield") should be (3.0f)
          row.getAs[Double]("doublefield") should be (37.8)
          row.getAs[Boolean]("booleanfield") should be (false)
          row.fieldIsNull("booleanfield2") should be (true)
        case _ => fail()
      }}
    }
  
    it("should set default values for missing fields according to schema") {
      val schemaWithDefaults = List(
        AvroFieldConfig( StructField("id",  StringType, nullable=false), 
          JString("")),
        AvroFieldConfig( StructField("intfield", IntegerType, nullable=true),
          JInt(1)),
        AvroFieldConfig( StructField("doublefield", DoubleType, nullable=true), 
          JDouble(4.0)),
        AvroFieldConfig( StructField("booleanfield", BooleanType, nullable=true), 
          JBool(false))
      )
      val stSchema = StructType( schemaWithDefaults.map{ _.structField } )

      val fullSchemaWithDefaults = schemaWithDefaults ++ List(
        AvroFieldConfig( StructField("longfield", LongType, nullable=true), 
          JInt(2))
        //AvroFieldConfig( StructField("floatfield", FloatType, nullable=true), 
        //  JDouble(3.14))
      )

      val df = sqlCtx.createDataFrame( sc.parallelize(
        List(//             id  int   double boolean
          Row.fromSeq(List( "0", 7,    7.8,   true)),
          Row.fromSeq(List("10", 17,  null,  null)),
          Row.fromSeq(List("20", null,27.8,   true)),
          Row.fromSeq(List("30", 37,  37.8,   null)))),
        stSchema)

      val defaultsDF = setDefaultValues(df, fullSchemaWithDefaults)
      val rows = defaultsDF.collect

      rows.size should be (4)
      rows.foreach{ row => row.getAs[String]("id") match {
        case "0" => 
          row.getAs[Int]("intfield") should be (7)
          row.getAs[Long]("longfield") should be (2L)
          //row.getAs[Float]("floatfield") should be (3.14f)
          row.getAs[Double]("doublefield") should be (7.8)
          row.getAs[Boolean]("booleanfield") should be (true)
        case "10" => 
          row.getAs[Int]("intfield") should be (17)
          row.getAs[Long]("longfield") should be (2L)
          //row.getAs[Float]("floatfield") should be (3.14f)
          row.getAs[Double]("doublefield") should be (4.0)
          row.getAs[Boolean]("booleanfield") should be (false)
        case "20" => 
          row.getAs[Int]("intfield") should be (1)
          row.getAs[Long]("longfield") should be (2L)
          //row.getAs[Float]("floatfield") should be (3.14f)
          row.getAs[Double]("doublefield") should be (27.8)
          row.getAs[Boolean]("booleanfield") should be (true)
        case "30" => 
          row.getAs[Int]("intfield") should be (37)
          row.getAs[Long]("longfield") should be (2L)
          //row.getAs[Float]("floatfield") should be (3.14f)
          row.getAs[Double]("doublefield") should be (37.8)
          row.getAs[Boolean]("booleanfield") should be (false)
        case _ => fail()
      }}
    }

    it("should not change the dataframe schema as a result of its operation")
    {
      // avro schema
      val jsonAvroSchema = """{
          "namespace": "ALS",
          "type": "record",
          "name": "impression",
          "fields": [{
            "name": "StringField",
            "type": "string",
            "default": ""
          }, {
            "name": "IntField",
            "type": [ "int" ],
            "default": 0
          }, {
            "name": "IntField2",
            "type": [ "null", "int" ],
            "default": 0
          }, {
            "name": "LongField",
            "type": [ "null", "long" ],
            "default": 0
          }, {
            "name": "DoubleField",
            "type": [ "null", "double" ],
            "default": 0.0
          }, {
            "name": "DoubleField2",
            "type": [ "null", "double" ],
            "default": 0.0
          }, {
            "name": "BooleanField",
            "type": [ "null", "boolean" ],
            "default": false
          }, {
            "name": "BooleanField2",
            "type": [ "null", "boolean" ],
            "default": false
          }
        ],
        "doc": ""
      }"""
      //      "name": "FloatField",
      //      "type": [ "null", "float" ],
      //      "default": 0.0
      //    }, {

      val schema = AvroSchema(jsonAvroSchema)

      val sfSchema = schema.toStructType
      val startDF = sqlCtx.createDataFrame( sc.parallelize(
        List(//             str int int lng, flt, dbl, dbl, bool,bool
          Row.fromSeq(List("STR", 1, 2, 3L, 4.1f, 5.1,-5.1, true, false)))),
        sfSchema)

      // check start schema
      def check(df: DataFrame) = {
        df.schema.fields(0).dataType should be (StringType)
        df.schema.fields(1).dataType should be (IntegerType)
        df.schema.fields(2).dataType should be (IntegerType)
        df.schema.fields(3).dataType should be (LongType)
        //df.schema.fields(4).dataType should be (FloatType)
        df.schema.fields(4).dataType should be (DoubleType)
        df.schema.fields(5).dataType should be (DoubleType)
        df.schema.fields(6).dataType should be (BooleanType)
        df.schema.fields(7).dataType should be (BooleanType)
      }
      check(startDF)

      val modDF = DataFrameUtil.setDefaultValues(startDF, schema.toListAvroFieldConfig)

      check(modDF)
    }
  }

}


