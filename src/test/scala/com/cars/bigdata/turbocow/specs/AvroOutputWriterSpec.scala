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

  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  // Tests start
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  describe("convertToType()") {
    
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
    }

    // null
    it("should properly convert Null types") {
      convertToType(null, StructField("", NullType, nullable=true)).get match { case null => ; case _ => fail() }
      convertToType(null, StructField("", NullType, nullable=false)).get match { case null => ; case _ => fail() }
    }
    it("should throw if null types have non-null value") {
      convertToType("", StructField("", NullType, nullable=true)).isSuccess should be (false)
      convertToType("", StructField("", NullType, nullable=false)).isSuccess should be (false)
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
            "type": [ "string" ],
            "default": ""
          }, {
            "name": "IntField",
            "type": [ "null", "int" ],
            "default": 0
          }, {
            "name": "LongField",
            "type": [ "null", "long" ],
            "default": 0
          }, {
            "name": "DoubleField",
            "type": [ "double" ],
            "default": 0.0
          }, {
            "name": "FloatField",
            "type": [ "null", "float" ],
            "default": 0.0
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

      val schema = AvroOutputWriter.getAvroSchema(avroSchema, sc)
      schema.size should be (7)
      schema.head should be (StructField("StringField", StringType, false))
      schema(1) should be (StructField("IntField", IntegerType, true))
      schema(2) should be (StructField("LongField", LongType, true))
      schema(3) should be (StructField("DoubleField", DoubleType, false))
      schema(4) should be (StructField("FloatField", FloatType, true))
      schema(5) should be (StructField("BooleanField", BooleanType, true))
      schema(6) should be (StructField("NullField", NullType, true))
    }
  }

  describe("write") {

    it("should only output the fields in the schema regardless of what is in the input RDD") {

      val avroSchema = """{
          "namespace": "ALS",
          "type": "record",
          "name": "impression",
          "fields": [
          {
            "name": "CField",
            "type": [ "string" ],
            "doc": "Type of the consumer activity. It is always IMPRESSION",
            "default": ""
          },
          {
            "name": "DField",
            "type": [ "null", "int" ],
            "doc": "Date when impression activity happened. Format of the date is yyyy-mm-dd",
            "default": 0
          }
        ],
        "doc": ""
      }"""
      val avroFile = writeTempFile(avroSchema, "avroschema.avsc")

      val enriched: RDD[Map[String, String]] = ActionEngine.processJsonStrings(
        List("""{ "md": { "AField": "A", "BField": "B" }, "activityMap": { "CField": "C", "DField": 11, "EField": true }}"""),
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
      enrichedAll.head.size should be (2)
      enrichedAll.head.get("BField") should be (Some("B"))
      enrichedAll.head.get("CField") should be (Some("C"))

      // now write to avro
      //val tempFile = File.createTempFile("testoutput-", ".avro", null).deleteOnExit()
      val outputDir = { 
        val dir = Files.createTempDirectory("testoutput-")
        new File(dir.toString).delete()
        dir.toString
      }
      println("%%%%%%%%%%%%%%%%%%%%%%%%% outputDir = "+outputDir.toString)

      // write
      AvroOutputWriter.write(enriched, avroFile, outputDir.toString, sc)

      // now read what we wrote
      val rows: Array[Row] = sqlCtx.read.avro(outputDir.toString).collect()
      //println("======== rows = ")
      rows.size should be (1) // one row only
      val row = rows.head
      row.size should be (2) // only one field in that row
      Try( row.getAs[String]("AField") ).isFailure should be (true)
      Try( row.getAs[String]("BField") ).isFailure should be (true)
      Try( row.getAs[String]("CField") ) should be (Success("C"))
    }

    it("should write out correct data types as specified in the schema") {

      // avro schema
      val avroSchema = """{
          "namespace": "ALS",
          "type": "record",
          "name": "impression",
          "fields": [{
            "name": "StringField",
            "type": [ "string" ],
            "default": ""
          }, {
            "name": "IntField",
            "type": [ "null", "int" ],
            "default": 0
          }, {
            "name": "IntField2",
            "type": [ "null", "int" ],
            "default": 0
          }, {
            "name": "DoubleField",
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
      val avroFile = writeTempFile(avroSchema, "avroschema.avsc")

      val enriched: RDD[Map[String, String]] = ActionEngine.processJsonStrings(
        // input record:
        List("""{ "md":{}, "activityMap": { 
            "StringField": "String", 
            "IntField": "10",
            "IntField2": "-10",
            "DoubleField": "10.1",
            "BooleanField": "true",
            "BooleanField2": "false"
          }}"""),
        // config: 
        """{
            "activityType": "impressions",
            "items": [
              {
                "actions":[{
                    "actionType":"simple-copy",
                    "config": {
                      "inputSource": [ "StringField", "IntField", "IntField2", "DoubleField", "BooleanField", "BooleanField2" ]
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
      enrichedAll.head.size should be (6)
      enrichedAll.head.get("StringField") should be (Some("String"))
      enrichedAll.head.get("IntField") should be (Some("10"))
      enrichedAll.head.get("IntField2") should be (Some("-10"))
      enrichedAll.head.get("DoubleField") should be (Some("10.1"))
      enrichedAll.head.get("BooleanField") should be (Some("true"))
      enrichedAll.head.get("BooleanField2") should be (Some("false"))

      // now write to avro
      //val tempFile = File.createTempFile("testoutput-", ".avro", null).deleteOnExit()
      val outputDir = { 
        val dir = Files.createTempDirectory("testoutput-")
        new File(dir.toString).delete()
        dir.toString
      }
      println("%%%%%%%%%%%%%%%%%%%%%%%%% outputDir = "+outputDir.toString)

      // write
      AvroOutputWriter.write(enriched, avroFile, outputDir.toString, sc)

      // now read what we wrote
      val rows: Array[Row] = sqlCtx.read.avro(outputDir.toString).collect()
      //println("======== rows = ")
      rows.size should be (1) // one row only
      val row = rows.head
      row.size should be (6)

      // these are all actual non-string types (except the first)
      Try( row.getAs[String]("StringField") ) should be (Success("String"))
      Try( row.getAs[Int]("IntField") ) should be (Success(10))
      Try( row.getAs[Int]("IntField2") ) should be (Success(-10))
      Try( row.getAs[Double]("DoubleField") ) should be (Success(10.1))
      Try( row.getAs[Boolean]("BooleanField") ) should be (Success(true))
      Try( row.getAs[Boolean]("BooleanField2") ) should be (Success(false))
    }

    it("should write out default values as specified in the schema") {
      fail()
    }
  }

}

