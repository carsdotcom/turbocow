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

class AvroOutputWriterSpec
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

  def fieldIsNull(row: Row, fieldName: String): Boolean = {
    val index = row.fieldIndex(fieldName)
    ( index >= row.size || row.isNullAt(index) )
  }

  sc.setLogLevel("WARN") 

  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  // Tests start
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  describe("convertToType()") {
    
    it("should throw a EmptyStringConversionException if converting a blank string to number or boolean") {
      intercept[EmptyStringConversionException]{ convertToType("", StructField("", IntegerType)).get }
      intercept[EmptyStringConversionException]{ convertToType("", StructField("", LongType)).get }
      intercept[EmptyStringConversionException]{ convertToType("", StructField("", FloatType)).get }
      intercept[EmptyStringConversionException]{ convertToType("", StructField("", DoubleType)).get }
      intercept[EmptyStringConversionException]{ convertToType("", StructField("", BooleanType)).get }
    }

    it("should throw a EmptyStringConversionException if converting an only-spaces string to number or boolean") {
      intercept[EmptyStringConversionException]{ convertToType(" ", StructField("", IntegerType)).get }
      intercept[EmptyStringConversionException]{ convertToType("  ", StructField("", LongType)).get }
      intercept[EmptyStringConversionException]{ convertToType("   ", StructField("", FloatType)).get }
      intercept[EmptyStringConversionException]{ convertToType(" ", StructField("", DoubleType)).get }
      intercept[EmptyStringConversionException]{ convertToType(" ", StructField("", BooleanType)).get }
    }

    it("should not throw when converting empty string or strings with spaces") {
      convertToType("", StructField("", StringType)).isSuccess should be (true)
      convertToType("  ", StructField("", StringType)).isSuccess should be (true)
    }

    it("should properly convert String types") {
      convertToType("testStr", StructField("", StringType)).get should be("testStr")
      convertToType("", StructField("", StringType)).get should be("")
      convertToType(" ", StructField("", StringType)).get should be(" ")

      convertToType(null, StructField("", StringType, nullable=true)).isSuccess should be (true)
      convertToType(null, StructField("", StringType, nullable=false)).isSuccess should be (false)
    }

    // Int
    it("should properly convert Integer types") {
      convertToType("10", StructField("", IntegerType)).get should be(10)
      convertToType(" 10", StructField("", IntegerType)).get should be(10)
      convertToType("10 ", StructField("", IntegerType)).get should be(10)
      convertToType(" 10 ", StructField("", IntegerType)).get should be(10)
      convertToType("-11", StructField("", IntegerType)).get should be(-11)
      convertToType("010", StructField("", IntegerType)).get should be(10)
    }
    it("should throw if string value is over/under max/min values for Integer types") {
      convertToType((Int.MaxValue.toLong).toString, StructField("", IntegerType)).get should be (Int.MaxValue)
      convertToType((Int.MaxValue.toLong +1L).toString, StructField("", IntegerType)).isSuccess should be (false)

      convertToType((Int.MinValue.toLong).toString, StructField("", IntegerType)).get should be (Int.MinValue)
      convertToType((Int.MinValue.toLong -1L).toString, StructField("", IntegerType)).isSuccess should be (false)
    }
    it("should throw if unparseable Int value") {
      convertToType("X123", StructField("", IntegerType)).isSuccess should be (false)
      convertToType("1X3", StructField("", IntegerType)).isSuccess should be (false)
      convertToType("123X", StructField("", IntegerType)).isSuccess should be (false)
      convertToType("0x10", StructField("", IntegerType)).isSuccess should be (false)

      convertToType("10d", StructField("", IntegerType)).isSuccess should be (false)
      convertToType("10D", StructField("", IntegerType)).isSuccess should be (false)
      convertToType("10f", StructField("", IntegerType)).isSuccess should be (false)
      convertToType("10F", StructField("", IntegerType)).isSuccess should be (false)
      convertToType("10l", StructField("", IntegerType)).isSuccess should be (false)
      convertToType("10L", StructField("", IntegerType)).isSuccess should be (false)

      convertToType("10.1", StructField("", IntegerType)).isSuccess should be (false)

      // blanks should fail
      convertToType("", StructField("", IntegerType)).isSuccess should be (false)
      convertToType(" ", StructField("", IntegerType)).isSuccess should be (false)

      // nulls should fail unless it is nullable
      convertToType(null, StructField("", IntegerType, nullable = false)).isSuccess should be (false)
      convertToType(null, StructField("", IntegerType, nullable = true)).get match { case null =>; case _ => fail() }
    }

    // Long
    it("should properly convert Long types") {
      convertToType("10", StructField("", LongType)).get should be(10L)
      convertToType(" 10", StructField("", LongType)).get should be(10L)
      convertToType("10 ", StructField("", LongType)).get should be(10L)
      convertToType(" 10 ", StructField("", LongType)).get should be(10L)
      convertToType("-11", StructField("", LongType)).get should be(-11L)
      convertToType("010", StructField("", LongType)).get should be(10L)
    }
    it("should throw if string value is over/under max/min values for Long types") {
      convertToType("9223372036854775807", StructField("", LongType)).get should be (Long.MaxValue)
      convertToType("9223372036854775808", StructField("", LongType)).isSuccess should be (false)

      convertToType("-9223372036854775808", StructField("", LongType)).get should be (Long.MinValue)
      convertToType("-9223372036854775809", StructField("", LongType)).isSuccess should be (false)
    }
    it("should throw if unparseable Long value") {
      convertToType("X123", StructField("", LongType)).isSuccess should be (false)
      convertToType("1X3", StructField("", LongType)).isSuccess should be (false)
      convertToType("123X", StructField("", LongType)).isSuccess should be (false)
      convertToType("0x10", StructField("", LongType)).isSuccess should be (false)

      convertToType("10d", StructField("", LongType)).isSuccess should be (false)
      convertToType("10D", StructField("", LongType)).isSuccess should be (false)
      convertToType("10f", StructField("", LongType)).isSuccess should be (false)
      convertToType("10F", StructField("", LongType)).isSuccess should be (false)
      convertToType("10l", StructField("", LongType)).isSuccess should be (false)
      convertToType("10L", StructField("", LongType)).isSuccess should be (false)

      convertToType("10.1", StructField("", LongType)).isSuccess should be (false)

      // blanks should fail
      convertToType("", StructField("", LongType)).isSuccess should be (false)
      convertToType(" ", StructField("", LongType)).isSuccess should be (false)

      // null should fail unless it is nullable
      convertToType(null, StructField("", LongType, nullable = false)).isSuccess should be (false)
      convertToType(null, StructField("", LongType, nullable = true)).get match { case null =>; case _ => fail() }
    }

    // Float
    it("should properly convert Float types") {
      convertToType("-10.2", StructField("", FloatType)).get should be (-10.2f)
      convertToType(" -10.2", StructField("", FloatType)).get should be (-10.2f)
      convertToType("-10.2 ", StructField("", FloatType)).get should be (-10.2f)
      convertToType(" -10.2 ", StructField("", FloatType)).get should be (-10.2f)
    }
    it("should throw if unparseable Float value") {
      convertToType("X123.0", StructField("", FloatType)).isSuccess should be (false)
      convertToType("1X3.1", StructField("", FloatType)).isSuccess should be (false)
      convertToType("123.1X", StructField("", FloatType)).isSuccess should be (false)
      convertToType("0x10.1", StructField("", FloatType)).isSuccess should be (false)

      convertToType("10.1d", StructField("", FloatType)).isSuccess should be (false)
      convertToType("10.1D", StructField("", FloatType)).isSuccess should be (false)
      convertToType("10.1f", StructField("", FloatType)).isSuccess should be (false)
      convertToType("10.1F", StructField("", FloatType)).isSuccess should be (false)
      convertToType("10.1l", StructField("", FloatType)).isSuccess should be (false)
      convertToType("10.1L", StructField("", FloatType)).isSuccess should be (false)

      // blanks should fail
      convertToType("", StructField("", FloatType)).isSuccess should be (false)
      convertToType(" ", StructField("", FloatType)).isSuccess should be (false)

      // nulls should fail unless it is nullable
      convertToType(null, StructField("", FloatType, nullable = false)).isSuccess should be (false)
      convertToType(null, StructField("", FloatType, nullable = true)).get match { case null =>; case _ => fail() }
    }

    // Double
    it("should properly convert Double types") {
      convertToType("-10.2", StructField("", DoubleType)).get should be (-10.2)
      convertToType(" -10.2", StructField("", DoubleType)).get should be (-10.2)
      convertToType("-10.2 ", StructField("", DoubleType)).get should be (-10.2)
      convertToType(" -10.2 ", StructField("", DoubleType)).get should be (-10.2)
    }
    it("should throw if unparseable Double value") {
      convertToType("X123.0", StructField("", DoubleType)).isSuccess should be (false)
      convertToType("1X3.1", StructField("", DoubleType)).isSuccess should be (false)
      convertToType("123.1X", StructField("", DoubleType)).isSuccess should be (false)
      convertToType("0x10.1", StructField("", DoubleType)).isSuccess should be (false)

      convertToType("10.1d", StructField("", DoubleType)).isSuccess should be (false)
      convertToType("10.1D", StructField("", DoubleType)).isSuccess should be (false)
      convertToType("10.1f", StructField("", DoubleType)).isSuccess should be (false)
      convertToType("10.1F", StructField("", DoubleType)).isSuccess should be (false)
      convertToType("10.1l", StructField("", DoubleType)).isSuccess should be (false)
      convertToType("10.1L", StructField("", DoubleType)).isSuccess should be (false)

      // blanks should fail
      convertToType("", StructField("", DoubleType)).isSuccess should be (false)
      convertToType(" ", StructField("", DoubleType)).isSuccess should be (false)

      // nulls should fail unless it is nullable
      convertToType(null, StructField("", DoubleType, nullable = false)).isSuccess should be (false)
      convertToType(null, StructField("", DoubleType, nullable = true)).get match { case null =>; case _ => fail() }
    }

    // Boolean
    it("should properly convert Boolean types") {
      convertToType("true", StructField("", BooleanType)).get match { case b: Boolean => b should be (true); case _ => fail() }
      convertToType("false", StructField("", BooleanType)).get match { case b: Boolean => b should be (false); case _ => fail() }
      convertToType(" true", StructField("", BooleanType)).get match { case b: Boolean => b should be (true); case _ => fail() }
      convertToType(" false", StructField("", BooleanType)).get match { case b: Boolean => b should be (false); case _ => fail() }
      convertToType("true ", StructField("", BooleanType)).get match { case b: Boolean => b should be (true); case _ => fail() }
      convertToType("false ", StructField("", BooleanType)).get match { case b: Boolean => b should be (false); case _ => fail() }
      convertToType(" true ", StructField("", BooleanType)).get match { case b: Boolean => b should be (true); case _ => fail() }
      convertToType(" false ", StructField("", BooleanType)).get match { case b: Boolean => b should be (false); case _ => fail() }

      convertToType("xfalse", StructField("", BooleanType)).isSuccess should be (false)
      convertToType("falsex", StructField("", BooleanType)).isSuccess should be (false)
      convertToType("falxse", StructField("", BooleanType)).isSuccess should be (false)
      convertToType("atrue", StructField("", BooleanType)).isSuccess should be (false)
      convertToType("tru", StructField("", BooleanType)).isSuccess should be (false)
      convertToType("10", StructField("", BooleanType)).isSuccess should be (false)

      // blanks should fail
      convertToType("", StructField("", BooleanType)).isSuccess should be (false)
      convertToType(" ", StructField("", BooleanType)).isSuccess should be (false)

      // nulls should fail unless it is nullable
      convertToType(null, StructField("", BooleanType, nullable = false)).isSuccess should be (false)
      convertToType(null, StructField("", BooleanType, nullable = true)).get match { case null =>; case _ => fail() }
    }

    // null
    it("should properly convert Null types") {
      convertToType(null, StructField("", NullType, nullable=true)).get match { case null => ; case _ => fail() }
      convertToType(null, StructField("", NullType, nullable=false)).get match { case null => ; case _ => fail() }
    }
    it("should throw if null types have non-null value") {
      convertToType("", StructField("", NullType, nullable=true)).isSuccess should be (false)
      convertToType("", StructField("", NullType, nullable=false)).isSuccess should be (false)

      convertToType(" ", StructField("", NullType, nullable=false)).isSuccess should be (false)

      convertToType("X", StructField("", NullType, nullable=true)).isSuccess should be (false)
      convertToType("X", StructField("", NullType, nullable=false)).isSuccess should be (false)
    }
  }

  describe("getDataTypeFromString") {

    it("should return the correct type") {
      getDataTypeFromString("string") should be (StringType)
      getDataTypeFromString("int") should be (IntegerType)
      getDataTypeFromString("long") should be (LongType)
      getDataTypeFromString("double") should be (DoubleType)
      getDataTypeFromString("float") should be (FloatType)
      getDataTypeFromString("boolean") should be (BooleanType)
      getDataTypeFromString("null") should be (NullType)
    }
  }

  describe("getAvroSchema") {

    it("should return correct types from the schema") {
      val avroSchema = """{
        "namespace": "ALS",
        "type": "record",
        "name": "impression",
        "fields": [{
            "name": "StringField",
            "type": "string",
            "default": "0"
          }, {
            "name": "IntField",
            "type": [ "null", "int" ],
            "default": 1
          }, {
            "name": "LongField",
            "type": [ "null", "long" ],
            "default": 2
          }, {
            "name": "DoubleField",
            "type": [ "double" ],
            "default": 0.3
          }, {
            "name": "BooleanField",
            "type": [ "null", "boolean" ],
            "default": false
          }, {
            "name": "NullField",
            "type": [ "null" ],
            "default": null
          }
        ],
        "doc": ""
      }"""

      val schema = AvroOutputWriter.getAvroSchema(avroSchema)
      schema.size should be (6)
      schema.head should be (AvroFieldConfig(StructField("StringField", StringType, false), JString("0")))
      schema(1) should be (AvroFieldConfig(StructField("IntField", IntegerType, true), JInt(1)))
      schema(2) should be (AvroFieldConfig(StructField("LongField", LongType, true), JInt(2)))
      schema(3) should be (AvroFieldConfig(StructField("DoubleField", DoubleType, false), JDouble(0.3)))
      schema(4) should be (AvroFieldConfig(StructField("BooleanField", BooleanType, true), JBool(false)))
      schema(5) should be (AvroFieldConfig(StructField("NullField", NullType, true), JNull))
    }

    it("""should throw if attempting to use a Float field - Floats are not 
          allowed due to https://issues.apache.org/jira/browse/SPARK-14081""") {

      val avroSchema = """{
        "namespace": "ALS",
        "type": "record",
        "name": "impression",
        "fields": [{
            "name": "StringField",
            "type": "string",
            "default": "0"
          }, {
            "name": "FloatField",
            "type": [ "null", "float" ],
            "default": 0.1
          }, {
            "name": "DoubleField",
            "type": [ "double" ],
            "default": 0.2
          }
        ],
        "doc": ""
      }"""

      intercept[Exception]{ AvroOutputWriter.getAvroSchema(avroSchema) }
    }
  }

  describe("writeEnrichedRDD()") {

    it("should only output the fields in the schema regardless of what is in the input RDD") {

      // schema has C & D
      val avroSchema = """{
          "namespace": "ALS",
          "type": "record",
          "name": "impression",
          "fields": [
          {
            "name": "CField",
            "type": [ "string" ],
            "default": ""
          },
          {
            "name": "DField",
            "type": [ "null", "int" ],
            "default": 0
          }
        ],
        "doc": ""
      }"""
      val avroFile = writeTempFile(avroSchema, "avroschema.avsc")
      // check we wrote to file correctly
      parse(Source.fromFile(avroFile).getLines.mkString).toString should be ((parse(avroSchema)).toString)

      val enriched: RDD[Map[String, String]] = ActionEngine.processJsonStrings(
        // Input has A-E
        List("""{ "md": { "AField": "A", "BField": "B" }, "activityMap": { "CField": "C", "DField": 11, "EField": true }}"""),
        // Config has B & C
        """{
            "activityType": "impressions",
            "items": [
              {
                "actions":[{
                    "actionType":"simple-copy",
                    "config": {
                      "inputSource": [ "BField", "CField" ]
                    }
                  }
                ]
              }
            ]
          }
        """,
        sc).persist()
  
      // this should be the enriched record:
      val enrichedAll = enriched.collect()
      //println("========= enrichedAll = "+enrichedAll.mkString("//"))
      enrichedAll.size should be (1) // always one because there's only one json input object
      enrichedAll.head.size should be (5) // all fields are copied by default if not otherwise added
      enrichedAll.head.get("BField") should be (Some("B"))
      enrichedAll.head.get("CField") should be (Some("C"))

      // now write to avro
      //val tempFile = File.createTempFile("testoutput-", ".avro", null).deleteOnExit()
      val outputDir = { 
        val dir = Files.createTempDirectory("testoutput-")
        new File(dir.toString).delete()
        dir.toString
      }

      // write
      (new AvroOutputWriter(sc)).writeEnrichedRDD(enriched, avroFile, sqlCtx, outputDir.toString)

      // now read what we wrote - should only have the union, field C
      val rows: Array[Row] = sqlCtx.read.avro(outputDir.toString).collect()
      //println("======== rows = ")
      rows.size should be (1) // one row only
      val row = rows.head
      row.size should be (2) // only one field in that row
      Try( row.getAs[String]("AField") ).isSuccess should be (false)
      Try( row.getAs[String]("BField") ).isSuccess should be (false)
      Try( row.getAs[String]("CField") ) should be (Success("C"))
      // this will actually exist but it defaults the value.  we are not testing 
      // defaults in this test so I'm not testing the value.  
      // See below for default values tests.
      Try( row.getAs[Int]("DField") ).isSuccess should be (true)
      Try( row.getAs[String]("EField") ).isSuccess should be (false)
    }

    it("should write out correct data types as specified in the schema") {

      // avro schema
      val avroSchema = """{
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
      //    }, {
      //      "name": "FloatField",
      //      "type": [ "null", "float" ],
      //      "default": 0.0

      val avroFile = writeTempFile(avroSchema, "avroschema.avsc")

      val enriched: RDD[Map[String, String]] = ActionEngine.processJsonStrings(
        // input record:
        List("""{ "md":{}, "activityMap": { 
            "StringField": "String", 
            "IntField": "10",
            "IntField2": "-10",
            "LongField": "11",
            "DoubleField": "10.1",
            "DoubleField2": "-10.1",
            "BooleanField": "true",
            "BooleanField2": "false"
          }}"""),
            //"FloatField": "-11.1",
        // config: 
        """{
            "activityType": "impressions",
            "items": [
              {
                "actions":[{
                    "actionType":"simple-copy",
                    "config": {
                      "inputSource": [ "StringField", "IntField", "IntField2", "LongField", "DoubleField", "DoubleField2", "BooleanField", "BooleanField2" ]
                    }
                  }
                ]
              }
            ]
          }
        """,
//                      "inputSource": [ "StringField", "IntField", "IntField2", "LongField", "FloatField", "DoubleField", "DoubleField2", "BooleanField", "BooleanField2" ]
        sc).persist()
  
      // this should be the enriched record:

      val enrichedAll = enriched.collect()
      //println("========= enrichedAll = "+enrichedAll.mkString("//"))
      enrichedAll.size should be (1) // always one because there's only one json input object
      enrichedAll.head.size should be (8)
      enrichedAll.head.get("StringField") should be (Some("String"))
      enrichedAll.head.get("IntField") should be (Some("10"))
      enrichedAll.head.get("IntField2") should be (Some("-10"))
      enrichedAll.head.get("LongField") should be (Some("11"))
      //enrichedAll.head.get("FloatField") should be (Some("-11.1"))
      enrichedAll.head.get("DoubleField") should be (Some("10.1"))
      enrichedAll.head.get("DoubleField2") should be (Some("-10.1"))
      enrichedAll.head.get("BooleanField") should be (Some("true"))
      enrichedAll.head.get("BooleanField2") should be (Some("false"))

      // now write to avro
      //val tempFile = File.createTempFile("testoutput-", ".avro", null).deleteOnExit()
      val outputDir = { 
        val dir = Files.createTempDirectory("testoutput-")
        new File(dir.toString).delete()
        dir.toString
      }

      // write
      (new AvroOutputWriter(sc)).writeEnrichedRDD(enriched, avroFile, sqlCtx, outputDir.toString)

      // now read what we wrote
      val rows: Array[Row] = sqlCtx.read.avro(outputDir.toString).collect()
      //println("======== rows = ")
      rows.size should be (1) // one row only
      val row = rows.head
      row.size should be (8)

      // these are all actual non-string types (except the first)
      Try( row.getAs[String]("StringField") ) should be (Success("String"))
      Try( row.getAs[Int]("IntField") ) should be (Success(10))
      Try( row.getAs[Int]("IntField2") ) should be (Success(-10))
      Try( row.getAs[Long]("LongField") ) should be (Success(11L))
      //Try( row.getAs[Float]("FloatField") ) should be (Success(-11.1f))
      Try( row.getAs[Double]("DoubleField") ) should be (Success(10.1))
      Try( row.getAs[Double]("DoubleField2") ) should be (Success(-10.1))
      Try( row.getAs[Boolean]("BooleanField") ) should be (Success(true))
      Try( row.getAs[Boolean]("BooleanField2") ) should be (Success(false))
    }

    it("should write out default values as specified in the schema") {

      // avro schema
      val avroSchema = """{
          "namespace": "ALS",
          "type": "record",
          "name": "impression",
          "fields": [{
            "name": "StringField",
            "type": "string",
            "default": "0"
          }, {
            "name": "IntField",
            "type": [ "int" ],
            "default": 1
          }, {
            "name": "IntField2",
            "type": [ "null", "int" ],
            "default": null
          }, {
            "name": "LongField",
            "type": [ "null", "long" ],
            "default": 3
          }, {
            "name": "DoubleField",
            "type": [ "null", "double" ],
            "default": 5.0
          }, {
            "name": "DoubleField2",
            "type": [ "null", "double" ],
            "default": null
          }, {
            "name": "BooleanField",
            "type": [ "null", "boolean" ],
            "default": false
          }, {
            "name": "BooleanField2",
            "type": [ "null", "boolean" ],
            "default": null
          }
        ],
        "doc": ""
      }"""
      //      "name": "FloatField",
      //      "type": [ "null", "float" ],
      //      "default": 4.0
      //    }, {

      val avroFile = writeTempFile(avroSchema, "avroschema.avsc")

      val enriched: RDD[Map[String, String]] = ActionEngine.processJsonStrings(
        // input record:
        List("""{ "md":{}, "activityMap": { 
            "UnknownField": "String"
          }}"""),
        // config:  no fields added from schema
        """{
            "activityType": "impressions",
            "items": [
              {
                "actions":[{
                    "actionType":"add-enriched-field",
                    "config": [{
                      "key": "X",
                      "value": "XVal"
                    }]
                  }
                ]
              }
            ]
          }
        """,
        sc).persist()
  
      // this should be the enriched record:

      val enrichedAll = enriched.collect()
      //println("========= enrichedAll = "+enrichedAll.mkString("//"))
      enrichedAll.size should be (1)
      enrichedAll.head.size should be (2) // input fields are added if not processed
      enrichedAll.head.get("X") should be (Some("XVal"))
    
      // now write to avro
      val outputDir = {
        val dir = Files.createTempDirectory("testoutput-")
        new File(dir.toString).delete()
        dir.toString
      }
    
      // write
      (new AvroOutputWriter(sc)).writeEnrichedRDD(enriched, avroFile, sqlCtx, outputDir.toString)
    
      // now read what we wrote
      val rows: Array[Row] = sqlCtx.read.avro(outputDir.toString).collect()
      //println("======== rows = ")
      rows.size should be (1) // one row only
      val row = rows.head
      row.size should be (8)

      // these are all actual non-string types (except the first)
      Try( row.getAs[String]("StringField") )    should be (Success("0"))
      Try( row.getAs[Int]("IntField") )          should be (Success(1))
      row.isNullAt( row.fieldIndex("IntField2") ) should be (true)
      Try( row.getAs[Long]("LongField") )        should be (Success(3L))
      //Try( row.getAs[Float]("FloatField") )      should be (Success(4.0))
      Try( row.getAs[Double]("DoubleField") )    should be (Success(5.0))
      row.isNullAt( row.fieldIndex("DoubleField2") ) should be (true)
      Try( row.getAs[Boolean]("BooleanField") )  should be (Success(false))
      row.isNullAt( row.fieldIndex("BooleanField2") ) should be (true)
    }

    it("should write out default values for numerics and booleans on blank input") {

      // avro schema
      val avroSchema = """{
          "namespace": "ALS",
          "type": "record",
          "name": "impression",
          "fields": [{
            "name": "IntField",
            "type": [ "null", "int" ],
            "default": 1
          }, {
            "name": "LongField",
            "type": [ "null", "long" ],
            "default": 3
          }, {
            "name": "DoubleField",
            "type": [ "null", "double" ],
            "default": 5.0
          }, {
            "name": "BooleanField",
            "type": [ "null", "boolean" ],
            "default": false
          }
        ],
        "doc": ""
      }"""
      //      "name": "FloatField",
      //      "type": [ "null", "float" ],
      //      "default": 4.0
      //    }, {

      val avroFile = writeTempFile(avroSchema, "avroschema.avsc")

      val enriched: RDD[Map[String, String]] = ActionEngine.processJsonStrings(
        // input record:
        List("""{ "md":{}, "activityMap": { 
            "IntField": "",
            "LongField": "",
            "DoubleField": "",
            "BooleanField": ""
          }}"""),
           // "FloatField": "",
        // config
        """{
            "activityType": "impressions",
            "items": [
              {
                "actions":[{
                    "actionType":"simple-copy",
                    "config": {
                      "inputSource": ["IntField", "LongField", "DoubleField", "BooleanField"]
                    }
                  }
                ]
              }
            ]
          }
        """,
        //              "inputSource": ["IntField", "LongField", "FloatField", "DoubleField", "BooleanField"]

        sc).persist()
  
      // this should be the enriched record:

      val enrichedAll = enriched.collect()
      //println("========= enrichedAll = "+enrichedAll.mkString("//"))
      enrichedAll.size should be (1)
      enrichedAll.head.size should be (4)
      enrichedAll.head.get("IntField") should be (Some(""))
      enrichedAll.head.get("LongField") should be (Some(""))
      //enrichedAll.head.get("FloatField") should be (Some(""))
      enrichedAll.head.get("DoubleField") should be (Some(""))
      enrichedAll.head.get("BooleanField") should be (Some(""))

      // now write to avro
      val outputDir = {
        val dir = Files.createTempDirectory("testoutput-")
        new File(dir.toString).delete()
        dir.toString
      }

      // write
      (new AvroOutputWriter(sc)).writeEnrichedRDD(enriched, avroFile, sqlCtx, outputDir.toString)

      // now read what we wrote
      val rows: Array[Row] = sqlCtx.read.avro(outputDir.toString).collect()
      //println("======== rows = ")
      rows.size should be (1) // one row only
      val row = rows.head
      row.size should be (4)

      // The default values should have been provided because the inputs were all blank strings
      Try( row.getAs[Int]("IntField") )          should be (Success(1))
      Try( row.getAs[Long]("LongField") )        should be (Success(3L))
      //Try( row.getAs[Float]("FloatField") )      should be (Success(4.0))
      Try( row.getAs[Double]("DoubleField") )    should be (Success(5.0))
      Try( row.getAs[Boolean]("BooleanField") )  should be (Success(false))
    }

    it("should write out default values for numerics and booleans with only blanks in the input") {

      // avro schema
      val avroSchema = """{
          "namespace": "ALS",
          "type": "record",
          "name": "impression",
          "fields": [{
            "name": "IntField",
            "type": [ "null", "int" ],
            "default": 1
          }, {
            "name": "LongField",
            "type": [ "null", "long" ],
            "default": 3
          }, {
            "name": "DoubleField",
            "type": [ "null", "double" ],
            "default": 5.0
          }, {
            "name": "BooleanField",
            "type": [ "null", "boolean" ],
            "default": false
          }
        ],
        "doc": ""
      }"""
      //      "name": "FloatField",
      //      "type": [ "null", "float" ],
      //      "default": 4.0
      //    }, {

      val avroFile = writeTempFile(avroSchema, "avroschema.avsc")

      val enriched: RDD[Map[String, String]] = ActionEngine.processJsonStrings(
        // input record:
        List("""{ "md":{}, "activityMap": { 
            "IntField": " ",
            "LongField": "  ",
            "DoubleField": "     ",
            "BooleanField": "      "
          }}"""),
            //"FloatField": "   ",
        // config
        """{
            "activityType": "impressions",
            "items": [
              {
                "actions":[{
                    "actionType":"simple-copy",
                    "config": {
                      "inputSource": ["IntField", "LongField", "DoubleField", "BooleanField"]
                    }
                  }
                ]
              }
            ]
          }
        """,
//                      "inputSource": ["IntField", "LongField", "FloatField", "DoubleField", "BooleanField"]
        sc).persist()
  
      // this should be the enriched record:

      val enrichedAll = enriched.collect()
      //println("========= enrichedAll = "+enrichedAll.mkString("//"))
      enrichedAll.size should be (1)
      enrichedAll.head.size should be (4)
      enrichedAll.head.get("IntField").get.substring(0,1) should be (" ")
      enrichedAll.head.get("LongField").get.substring(0,1) should be (" ")
      //enrichedAll.head.get("FloatField").get.substring(0,1) should be (" ")
      enrichedAll.head.get("DoubleField").get.substring(0,1) should be (" ")
      enrichedAll.head.get("BooleanField").get.substring(0,1) should be (" ")

      // now write to avro
      val outputDir = {
        val dir = Files.createTempDirectory("testoutput-")
        new File(dir.toString).delete()
        dir.toString
      }

      // write
      (new AvroOutputWriter(sc)).writeEnrichedRDD(enriched, avroFile, sqlCtx, outputDir.toString)

      // now read what we wrote
      val rows: Array[Row] = sqlCtx.read.avro(outputDir.toString).collect()
      //println("======== rows = ")
      rows.size should be (1) // one row only
      val row = rows.head
      row.size should be (4)

      // The default values should have been provided because the inputs were all blank strings
      Try( row.getAs[Int]("IntField") )          should be (Success(1))
      Try( row.getAs[Long]("LongField") )        should be (Success(3L))
      //Try( row.getAs[Float]("FloatField") )      should be (Success(4.0))
      Try( row.getAs[Double]("DoubleField") )    should be (Success(5.0))
      Try( row.getAs[Boolean]("BooleanField") )  should be (Success(false))
    }

    it("""should return an RDD of rejected records that were not written due to 
          datatype conversion problems""") {

      // avro schema
      val avroSchema = """{
          "namespace": "ALS",
          "type": "record",
          "name": "impression",
          "fields": [
          {
            "name": "id",
            "type": "string",
            "default": ""
          }, {
            "name": "StringField",
            "type": [ "null", "string" ],
            "default": ""
          }, {
            "name": "IntField",
            "type": [ "null", "int" ],
            "default": 1
          }, {
            "name": "LongField",
            "type": [ "null", "long" ],
            "default": 3
          }, {
            "name": "DoubleField",
            "type": [ "null", "double" ],
            "default": 5.0
          }, {
            "name": "BooleanField",
            "type": [ "null", "boolean" ],
            "default": false
          }
        ],
        "doc": ""
      }"""
         //   "name": "FloatField",
         //   "type": [ "null", "float" ],
         //   "default": 4.0
         // }, {
      val avroFile = writeTempFile(avroSchema, "avroschema.avsc")

      val enriched: RDD[Map[String, String]] = ActionEngine.processJsonStrings(
        // input records:
        List("""{ "md":{}, "activityMap": { 
            "id": "r1",
            "IntField": "X1",
            "LongField": "1",
            "DoubleField": "1",
            "BooleanField": "true",
            "StringField": "String"
          }}""",
          """{ "md":{}, "activityMap": { 
            "id": "r2",
            "IntField": "1",
            "LongField": "X1",
            "DoubleField": "1",
            "BooleanField": "true",
            "StringField": "String"
          }}""",
          """{ "md":{}, "activityMap": {
            "id": "r4",
            "IntField": "1",
            "LongField": "1",
            "DoubleField": "X1",
            "BooleanField": "true",
            "StringField": "String"
          }}""",
          """{ "md":{}, "activityMap": { 
            "id": "r5",
            "IntField": "1",
            "LongField": "1",
            "DoubleField": "1",
            "BooleanField": "Xtrue",
            "StringField": "String"
          }}""",
          """{ "md":{}, "activityMap": { 
            "id": "r6",
            "IntField": "1",
            "LongField": "1",
            "DoubleField": "1",
            "BooleanField": "true",
            "StringField": "XString"
          }}"""),

        //"""{ "md":{}, "activityMap": {
        //    "id": "r3",
        //    "IntField": "1",
        //    "LongField": "1",
        //    "DoubleField": "1",
        //    "BooleanField": "true",
        //    "StringField": "String"
        //  }}""",

            //"FloatField": "1",

        // config
        """{
            "activityType": "impressions",
            "items": [
              {
                "actions":[{
                    "actionType":"simple-copy",
                    "config": {
                      "inputSource": ["id", "IntField", "LongField", "DoubleField", "BooleanField", "StringField"]
                    }
                  }
                ]
              }
            ]
          }
        """,
//                      "inputSource": ["id", "IntField", "LongField", "FloatField", "DoubleField", "BooleanField", "StringField"]
        sc).persist()

      //val fields = List("id", "IntField", "LongField", "FloatField", "DoubleField", "BooleanField", "StringField")
      val fields = List("id", "IntField", "LongField", "DoubleField", "BooleanField", "StringField")

      // this should be the enriched record:
      val enrichedAll = enriched.collect()
      //println("========= enrichedAll = "+enrichedAll.mkString("//"))
      enrichedAll.size should be (5)
      enrichedAll.foreach{ _.size should be (fields.size) }

      // now write to avro
      val outputDir = {
        val dir = Files.createTempDirectory("testoutput-")
        new File(dir.toString).delete()
        dir.toString
      }

      // write
      val rejectedRDD = (new AvroOutputWriter(sc)).writeEnrichedRDD(enriched, avroFile, sqlCtx, outputDir.toString)
      val rejects = rejectedRDD.collect()

      // all but one will be rejected.
      rejects.size should be (enrichedAll.size - 1)

      //println("HHHHHHHHHHHHHHHHHHHHHHHHH here's the rejects:")
      //rejects.foreach{ r => println(r.toString) }

      rejects.foreach{ r =>
        r.size should be (fields.size + 1) // because of the error marker
        val id = r.get("id").get
        val realFields = fields.tail

        // all fields should be present
        fields.foreach{ field =>
          r.get(field).nonEmpty should be (true)
        }
        r.get(avroTypeErrorMarker).nonEmpty should be (true)
        r.get(avroTypeErrorMarker).get.trim.nonEmpty should be (true)

        // for each field, if one field has an X, none of the others should:
        r.count{ case(k,v) => v.nonEmpty && v.substring(0,1) == "X" } should be (1)

        id match {
          case "r1" => {
            r.get("IntField") should be (Some("X1"))
          }
          case "r2" => {
            r.get("LongField") should be (Some("X1"))
          }
          //case "r3" => {
          //  r.get("FloatField") should be (Some("X1"))
          //}
          case "r4" => {
            r.get("DoubleField") should be (Some("X1"))
          }
          case "r5" => {
            r.get("BooleanField") should be (Some("Xtrue"))
          }
          case "r6" => {
            // this should not be rejected
            fail()
          }
          case a: Any => fail()
        }
      }
    }
  }

  describe("convertToAllStringsSchema()") {
    it("should properly convert to strings") {
      val inputSchemaJson = """{
        "namespace": "ALS",
        "type": "record",
        "name": "impression",
        "fields": [{
            "name": "StringField",
            "type": [ "string" ],
            "default": "0"
          }, {
            "name": "IntField",
            "type": [ "null", "int" ],
            "default": 1
          }, {
            "name": "LongField",
            "type": [ "null", "long" ],
            "default": 2
          }, {
            "name": "DoubleField",
            "type": [ "double" ],
            "default": 0.3
          }, {
            "name": "BooleanField",
            "type": [ "null", "boolean" ],
            "default": false
          }, {
            "name": "NullField",
            "type": [ "null" ],
            "default": null
          }, {
            "name": "LongNullDefault",
            "type": [ "null", "long" ],
            "default": null
          }, {
            "name": "StringNullDefault",
            "type": [ "null", "string" ],
            "default": null
          }
        ],
        "doc": ""
      }"""
      //      "name": "FloatField",
      //      "type": [ "null", "float" ],
      //      "default": 0.4
      //    }, {

      val inputSchema = AvroOutputWriter.getAvroSchema(inputSchemaJson)

      val schemaF = AvroOutputWriter.convertToAllStringSchema(inputSchema, forceNullable=false)
      schemaF(0) should be (
        AvroFieldConfig(StructField("StringField", StringType, false), JString("0")))
      schemaF(1) should be (
        AvroFieldConfig(StructField("IntField", StringType, true), JString("1")))
      schemaF(2) should be (
        AvroFieldConfig(StructField("LongField", StringType, true), JString("2")))
      schemaF(3) should be (
        AvroFieldConfig(StructField("DoubleField", StringType, false), JString("0.3")))
      //schemaF(4) should be (
      //  AvroFieldConfig(StructField("FloatField", StringType, true), JString("0.4")))
      schemaF(4) should be (
        AvroFieldConfig(StructField("BooleanField", StringType, true), JString("false")))
      schemaF(5) should be (
        AvroFieldConfig(StructField("NullField", StringType, true), JNull))
      schemaF(6) should be ( 
        AvroFieldConfig(StructField("LongNullDefault", StringType, true), JNull))
      schemaF(7) should be (
        AvroFieldConfig(StructField("StringNullDefault", StringType, true), JNull))

      val schemaT = AvroOutputWriter.convertToAllStringSchema(inputSchema, forceNullable=true)
      schemaT(0) should be (
        AvroFieldConfig(StructField("StringField", StringType, true), JString("0")))
      schemaT(1) should be (
        AvroFieldConfig(StructField("IntField", StringType, true), JString("1")))
      schemaT(2) should be (
        AvroFieldConfig(StructField("LongField", StringType, true), JString("2")))
      schemaT(3) should be (
        AvroFieldConfig(StructField("DoubleField", StringType, true), JString("0.3")))
      //schemaT(4) should be (
      //  AvroFieldConfig(StructField("FloatField", StringType, true), JString("0.4")))
      schemaT(4) should be (
        AvroFieldConfig(StructField("BooleanField", StringType, true), JString("false")))
      schemaT(5) should be (
        AvroFieldConfig(StructField("NullField", StringType, true), JNull))
      schemaF(6) should be ( 
        AvroFieldConfig(StructField("LongNullDefault", StringType, true), JNull))
      schemaF(7) should be (
        AvroFieldConfig(StructField("StringNullDefault", StringType, true), JNull))
    }
  }

  describe("convertEnrichedRDDToDataFrame()") {
  
    it("should copy over all fields when enrichedRDD matches schema exactly") {
      val enrichedRDD = sc.parallelize(List( 
        Map("A"->"A1", "B"->"1"),
        Map("A"->"A2", "B"->"2")
      ))
      val schema = List(
        AvroFieldConfig(StructField("A", StringType, nullable=true), JNull),
        AvroFieldConfig(StructField("B", IntegerType, nullable=true), JNull)
      )
  
      val (goodDF, badRDD) = convertEnrichedRDDToDataFrame(enrichedRDD, schema, sqlCtx)
      val rows = goodDF.collect()
  
      goodDF.count should be (2)
      val afs: Array[StructField] = goodDF.schema.fields
      afs.size should be (2)
      afs(0).name should be ("A")
      afs(0).dataType should be (StringType)
      afs(0).nullable should be (true)
      afs(1).name should be ("B")
      afs(1).dataType should be (IntegerType)
      afs(1).nullable should be (true)
  
      rows.size should be (2)
      rows.foreach{ row => row.getAs[String]("A") match {
        case "A1" => row.getAs[Int]("B") should be (1)
        case "A2" => row.getAs[Int]("B") should be (2)
        case _ => fail()
      }}
  
      badRDD.count should be (0)
    }
  
    it("should return rows with values that couldn't be converted ") {
      //this is handled by test "should return an RDD of rejected records that were not written due to datatype conversion problems"
      //consider refactoring tests, or not
    }
  
    it("should set any missing enrichedRDD fields to null (and not the default value)") {
      val enrichedRDD = sc.parallelize(List( 
        Map("A"->"A1", "B"->"1"),
        Map("A"->"A2")
      ))
      val schema = List(
        AvroFieldConfig(StructField("A", StringType, nullable=true), JNull),
        AvroFieldConfig(StructField("B", IntegerType, nullable=true), JInt(5))
      )
  
      val (goodDF, badRDD) = convertEnrichedRDDToDataFrame(enrichedRDD, schema, sqlCtx)
      val rows = goodDF.collect()
  
      goodDF.count should be (2)
      val afs: Array[StructField] = goodDF.schema.fields
      afs.size should be (2)
      afs(0).name should be ("A")
      afs(0).dataType should be (StringType)
      afs(0).nullable should be (true)
      afs(1).name should be ("B")
      afs(1).dataType should be (IntegerType)
      afs(1).nullable should be (true)
  
      rows.size should be (2)
      rows.foreach{ row => row.getAs[String]("A") match {
        case "A1" => row.getAs[Int]("B") should be (1)
        case "A2" => fieldIsNull(row, "B") should be (true)
        case _ => fail()
      }}
  
      badRDD.count should be (0)
    }
  
    it("should set any non-nullable fields to nullable in the returned DF") {
      val enrichedRDD = sc.parallelize(List( 
        Map("A"->"A1", "B"->"1"),
        Map("A"->"A2", "B"->"2")
      ))
      val schema = List(
        AvroFieldConfig(StructField("A", StringType, nullable=false), JString("ADEFAULT")),
        AvroFieldConfig(StructField("B", IntegerType, nullable=false), JInt(5))
      )
  
      val (goodDF, badRDD) = convertEnrichedRDDToDataFrame(enrichedRDD, schema, sqlCtx)
      val rows = goodDF.collect()
  
      goodDF.count should be (2)
      val afs: Array[StructField] = goodDF.schema.fields
      afs.size should be (2)
      afs(0).name should be ("A")
      afs(0).dataType should be (StringType)
      afs(0).nullable should be (true)
      afs(1).name should be ("B")
      afs(1).dataType should be (IntegerType)
      afs(1).nullable should be (true)
    }

    it("should NOT add any extra enrichedRDD fields to the output dataframe") {
      val enrichedRDD = sc.parallelize(List( 
        Map("A"->"A1", "B"->"1", "C"->"C1"),
        Map("A"->"A2", "B"->"2", "D"->"D2")
      ))
      val schema = List(
        AvroFieldConfig(StructField("A", StringType, nullable=true), JNull),
        AvroFieldConfig(StructField("B", IntegerType, nullable=true), JNull)
      )
  
      val (goodDF, badRDD) = convertEnrichedRDDToDataFrame(enrichedRDD, schema, sqlCtx)
      val rows = goodDF.collect()
  
      goodDF.count should be (2)
      val afs: Array[StructField] = goodDF.schema.fields
      afs.size should be (2)
      afs(0).name should be ("A")
      afs(0).dataType should be (StringType)
      afs(0).nullable should be (true)
      afs(1).name should be ("B")
      afs(1).dataType should be (IntegerType)
      afs(1).nullable should be (true)
  
      rows.size should be (2)
      rows.foreach{ row => 
        row.size should be (2)
        row.getAs[String]("A") match {
          case "A1" => row.getAs[Int]("B") should be (1)
          case "A2" => row.getAs[Int]("B") should be (2)
          case _ => fail()
        }
      }
    }
  
    it("should output empty DataFrame if input RDD is empty") {
      val enrichedRDD = sc.parallelize(List.empty[Map[String, String]])
      val schema = List(
        AvroFieldConfig(StructField("A", StringType, nullable=true), JNull),
        AvroFieldConfig(StructField("B", IntegerType, nullable=true), JNull)
      )
  
      val (goodDF, badRDD) = convertEnrichedRDDToDataFrame(enrichedRDD, schema, sqlCtx)
      goodDF.count should be (0)
    }
  
    it("should throw if schema is an empty List") {
      val enrichedRDD = sc.parallelize(List.empty[Map[String, String]])
      val schema = List.empty[AvroFieldConfig]

      intercept[Exception] { convertEnrichedRDDToDataFrame(enrichedRDD, schema, sqlCtx) }
    }

    it("should throw if schema is null") {
      val enrichedRDD = sc.parallelize(List.empty[Map[String, String]])
      val schema: List[AvroFieldConfig] = null

      intercept[Exception] { convertEnrichedRDDToDataFrame(enrichedRDD, schema, sqlCtx) }
    }
  }
  
}

