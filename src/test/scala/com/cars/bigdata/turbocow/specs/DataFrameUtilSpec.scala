package com.cars.bigdata.turbocow

import com.cars.bigdata.turbocow.test.SparkTestContext._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.json4s._
import DataFrameUtil._
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
      val df = startDF.addColumnWithDefaultValue(
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
      val df = startDF.addColumnWithDefaultValue(
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
      val df = startDF.addColumnWithDefaultValue(
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
      val df = startDF.addColumnWithDefaultValue(
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
      val df = startDF.addColumnWithDefaultValue(
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
      val df = startDF.addColumnWithDefaultValue(
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
      val df = startDF.addColumnWithDefaultValue(
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
      val df = startDF.addColumnWithDefaultValue(
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

      val defaultsDF = df.setDefaultValues(schemaWithDefaults)
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

      val defaultsDF = df.setDefaultValues(fullSchemaWithDefaults)
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

      val modDF = startDF.setDefaultValues(schema.toListAvroFieldConfig)

      check(modDF)
    }
  }

  describe("changeSchema()") {

    it("should copy over all columns when schemas and types match exactly (happy path)") {
      // avro schema
      val jsonAvroSchema = """{
          "namespace": "NS",
          "type": "record",
          "name": "impression",
          "fields": [{
            "name": "StringField", "type": [ "null", "string" ], "default": null
          }, {
            "name": "IntField", "type": [ "null", "int" ], "default": null
          }, {
            "name": "LongField", "type": [ "null", "long" ], "default": null
          }, {
            "name": "DoubleField", "type": [ "null", "double" ], "default": null
          }, {
            "name": "BooleanField", "type": [ "null", "boolean" ], "default": null
          }
        ],
        "doc": ""
      }"""
        //  }, {
        //    "name": "NullField", "type": "null", "default": null
      val schema = AvroSchema(jsonAvroSchema)
      val sfSchema = schema.toStructType

      val startDF = sqlCtx.createDataFrame( sc.parallelize(
        List(//             str int lng, dbl, bool
          Row.fromSeq(List("ID0", 1, 2L, 4.1, true)))),
        sfSchema)

      // check start schema
      def checkSchema(df: DataFrame) = {
        df.schema.fields(0).dataType should be (StringType)
        df.schema.fields(1).dataType should be (IntegerType)
        df.schema.fields(2).dataType should be (LongType)
        df.schema.fields(3).dataType should be (DoubleType)
        df.schema.fields(4).dataType should be (BooleanType)
      }
      checkSchema(startDF)
      startDF.schema.fields.size should be (5)

      val result = startDF.changeSchema(schema.toListAvroFieldConfig)

      checkSchema(result.goodDF)
      result.goodDF.schema.fields.size should be (6)
      result.goodDF.schema.fields(5).name should be (DataFrameUtil.changeSchemaErrorField)

      val rows = result.goodDF.collect
      rows.size should be (1)
      rows.foreach{ row => row.getAs[String]("StringField") match {
        case "ID0" => 
          row.getAs[Int]("IntField") should be (1)
          row.getAs[Long]("LongField") should be (2L)
          row.getAs[Double]("DoubleField") should be (4.1)
          row.getAs[Boolean]("BooleanField") should be (true)
          row.fieldIsNull(changeSchemaErrorField) should be (true)
      }}

      result.errorDF.count should be (0)
    }

    it("should set columns in schema that are missing in DF to null") {
      // avro schema
      val jsonAvroSchema = """{
          "namespace": "NS",
          "type": "record",
          "name": "impression",
          "fields": [{
            "name": "StringField", "type": [ "null", "string" ], "default": null
          }, {
            "name": "IntField", "type": [ "null", "int" ], "default": null
          }, {
            "name": "LongField", "type": [ "null", "long" ], "default": null
          }, {
            "name": "DoubleField", "type": [ "null", "double" ], "default": null
          }, {
            "name": "BooleanField", "type": [ "null", "boolean" ], "default": null
          }
        ],
        "doc": ""
      }"""
      // fullSchema has every field
      val fullSchema = AvroSchema(jsonAvroSchema)
      val fullSfSchema = fullSchema.toStructType

      // the test schema 'schema' is missing DoubleField
      val schema = fullSchema.copy(fields = fullSchema.fields.filterNot(_.name=="DoubleField"))
      val sfSchema = schema.toStructType

      val startDF = sqlCtx.createDataFrame( sc.parallelize(
        List(//             str int lng, bool    // NO DOUBLE
          Row.fromSeq(List("ID0", 1, 2L, true)))),
        sfSchema)

      // check start schema
      def checkSchema(df: DataFrame) = {
        df.schema.fields(0).dataType should be (StringType)
        df.schema.fields(1).dataType should be (IntegerType)
        df.schema.fields(2).dataType should be (LongType)
        //df.schema.fields(3).dataType should be (DoubleType)
        df.schema.fields(3).dataType should be (BooleanType)
      }
      checkSchema(startDF)
      startDF.schema.fields.size should be (4)

      // changing to fullSchema adds DoubleField
      val result = startDF.changeSchema(fullSchema.toListAvroFieldConfig)

      checkSchema(result.goodDF)
      result.goodDF.schema.fields.size should be (5)
      result.goodDF.schema.fields(4).name should be ("DoubleField")

      val rows = result.goodDF.collect
      rows.size should be (1)
      rows.foreach{ row => row.getAs[String]("StringField") match {
        case "ID0" => 
          row.getAs[Int]("IntField") should be (1)
          row.getAs[Long]("LongField") should be (2L)
          row.fieldIsNull("DoubleField") should be (true)
          row.getAs[Boolean]("BooleanField") should be (true)
      }}

      result.errorDF.count should be (0)
    }

    it("should remove columns in DF that are missing from schema") {
      fail()
    }

  }

  describe("changeSchema() where schemas are same except for one type") {

    val allTypes = List(StringType, IntegerType, LongType, DoubleType, BooleanType, NullType)
    // avro schema
    val schema = List(
      AvroFieldConfig( StructField("StringField", StringType, nullable=true), JNull),
      AvroFieldConfig( StructField("IntField", IntegerType, nullable=true), JNull),
      AvroFieldConfig( StructField("LongField", LongType, nullable=true), JNull),
      AvroFieldConfig( StructField("DoubleField", DoubleType, nullable=true), JNull),
      AvroFieldConfig( StructField("BooleanField", BooleanType, nullable=true), JNull),
      AvroFieldConfig( StructField("NullField", NullType, nullable=true), JNull)
    )

    it("should change String column types to other types correctly") {
          
      val fieldName = "StringField"
      val fieldIndex = schema.indexWhere( _.structField.name == fieldName )
      val oldType = StringType
      case class T(newType: DataType, testVal: String, expectedVal: Option[Any])
      val testVals = List(
        T(IntegerType, "X", None), 
        T(IntegerType, "2", Some(2)),
        T(LongType, "X", None), 
        T(LongType, "-4", Some(-4L)),
        T(DoubleType, "X", None), 
        T(DoubleType, "-6.1", Some(-6.1)),
        T(BooleanType, "", Some(false)), // NOTE this will change in spark 1.6 to be 'true/false/t/f'
        T(BooleanType, "true", Some(true))
      )

      testVals.foreach{ t =>
        println("TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT Testing testVal: "+t)
        val startStSchema = StructType(schema.map{ _.structField })
        val startDF = sqlCtx.createDataFrame( sc.parallelize(
          List(//             str      int lng,  dbl, bool, null
            Row.fromSeq(List(t.testVal, 10, 20L, 30.1, true, null)))),
          startStSchema)
        val newType = t.newType

        val newSchema = schema.map{ e => 
          if (e.structField.name == fieldName) {
            val name = e.structField.name
            println(s"Changing type of '${fieldName}' from '${oldType}' to '$newType'...")
            e.copy( structField = e.structField.copy(dataType = newType))
          }
          else e
        }

        val result = startDF.changeSchema(newSchema)
        val goodRows = result.goodDF.collect
        val errorRows = result.errorDF.collect

        if (t.expectedVal.nonEmpty) {
          // success
          val sf = result.goodDF.schema.fields.filter( _.name == fieldName )
          sf.size should be (1)
          sf.head.dataType should be (t.newType)

          errorRows.size should be (0)
          goodRows.size should be (1)

          val expected: Any = t.expectedVal.get
          val row = goodRows.head
          expected match {
            case null => row.fieldIsNull(fieldName) should be (true)
            case a: Any => row.get(row.fieldIndex(fieldName)) should be (a)
          }
        }
        else { // expected val is empty (error)

          result.errorDF should not be (None)
          val sf = result.errorDF.schema.fields.filter( _.name == fieldName )
          sf.size should be (1)
          sf.head.dataType should be (StringType)

          println("GGGGG good rows = ")
          result.goodDF.show()
          goodRows.size should be (0)

          errorRows.size should be (1)
          errorRows.head.getAs[String](fieldName) should be (t.testVal)
        }
      }
    }

    it("should change Int column types to other types correctly") {
      fail()
    }

    it("should change Long column types to other types correctly") {
      fail()
    }

    it("should change Double column types to other types correctly") {
      fail()
    }

    it("should change Boolean column types to other types correctly") {
      fail()
    }

    it("should change Null column types to other types correctly") {
      fail()
    }
  }

  describe("convertToAllStrings()") {

    import DataFrameUtil._
    val schema = StructType( Array(
      StructField("StringField", StringType, nullable=true),
      StructField("IntField", IntegerType, nullable=true),
      StructField("LongField", LongType, nullable=false),
      StructField("DoubleField", DoubleType, nullable=true),
      StructField("BooleanField", BooleanType, nullable=false),
      StructField("NullField", NullType, nullable=true)
    ))

    val startDF = sqlCtx.createDataFrame( sc.parallelize(
      List(//             str int   long  double boolean, null
        Row.fromSeq(List( "A",  1,   2L,   -3.1, true,    null)),
        Row.fromSeq(List( "B", 11,  12L,  -13.1, false,   null)))),
      schema)

    it("should change all data types to strings and properly convert all data") {
      val df = startDF.convertToAllStrings()

      // check schema:
      val fields = df.schema.fields
      fields.size should be (6)
      fields(0) should be (StructField("StringField", StringType, nullable=true))
      fields(1) should be (StructField("IntField", StringType, nullable=true))
      fields(2) should be (StructField("LongField", StringType, nullable=false))
      fields(3) should be (StructField("DoubleField", StringType, nullable=true))
      fields(4) should be (StructField("BooleanField", StringType, nullable=false))
      fields(5) should be (StructField("NullField", StringType, nullable=true))

      // check the data:
      val rows = df.collect
      rows.foreach{ row => row.getAs[String]("StringField") match {
        case "A" =>
          row.getAs[String]("IntField") should be ("1")
          row.getAs[String]("LongField") should be ("2")
          row.getAs[String]("DoubleField") should be ("-3.1")
          row.getAs[String]("BooleanField") should be ("true")
          row.fieldIsNull("NullField") should be (true)
        case "B" =>
          row.getAs[String]("IntField") should be ("11")
          row.getAs[String]("LongField") should be ("12")
          row.getAs[String]("DoubleField") should be ("-13.1")
          row.getAs[String]("BooleanField") should be ("false")
          row.fieldIsNull("NullField") should be (true)
      }}
    }

    it("should not change the nullable flag of each field") {
      val df = startDF.convertToAllStrings()

      // check schema:
      val fields = df.schema.fields
      fields.zipWithIndex.foreach{ case(f, index) =>
        println(s"index=$index; f = $f")
        f.nullable should be (schema.fields(index).nullable) 
      }
    }
  }
}


