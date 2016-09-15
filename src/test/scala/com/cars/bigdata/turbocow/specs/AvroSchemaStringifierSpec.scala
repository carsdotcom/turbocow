package com.cars.bigdata.turbocow

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.{read, write}

import scala.io.Source

import utils.FileUtil

class AvroSchemaStringifierSpec extends UnitSpec {

  implicit val jsonFormats = org.json4s.DefaultFormats
                    
  def writeFile(text: String, filename: String) = {
    FileUtil.writeFile(text, filename)
  }

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
    //myAfterEach()
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

  val testAvroSchemaString = """{
    "namespace": "ALS",
    "type": "record",
    "name": "impression",
    "doc": "global documentation",
    "fields": [{
        "name": "StringField",
        "type": [ "string" ],
        "doc": "StringFieldDoc",
        "default": "0"
      }, {
        "name": "IntField",
        "type": [ "null", "int" ],
        "doc": "IntFieldDoc",
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
        "name": "FloatField",
        "type": [ "null", "float" ],
        "default": 0.4
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
    ]
  }"""

  describe("convertToStringTypes()") {
    
    it("should convert an avro schema (as JSON string) to all-string types") {
      val modJson = AvroSchemaStringifier.convertToStringTypes(testAvroSchemaString)

      val a = AvroSchema(modJson)

      a.namespace should be ("ALS")
      a.`type` should be ("record")
      a.name should be ("impression")
      a.doc should be ("global documentation")
      a.fields.size should be (9)
      a.fields(0) should be (AvroSchemaField("StringField",       List("string"),         JString("0"), "StringFieldDoc"))
      a.fields(1) should be (AvroSchemaField("IntField",          List("null", "string"), JString("1"), "IntFieldDoc"))
      a.fields(2) should be (AvroSchemaField("LongField",         List("null", "string"), JString("2"), ""))
      a.fields(3) should be (AvroSchemaField("DoubleField",       List(        "string"), JString("0.3"), ""))
      a.fields(4) should be (AvroSchemaField("FloatField",        List("null", "string"), JString("0.4"), ""))
      a.fields(5) should be (AvroSchemaField("BooleanField",      List("null", "string"), JString("false"), ""))
      a.fields(6) should be (AvroSchemaField("NullField",         List("null"          ), JNull, ""))
      a.fields(7) should be (AvroSchemaField("LongNullDefault",   List("null", "string"), JNull, ""))
      a.fields(8) should be (AvroSchemaField("StringNullDefault", List("null", "string"), JNull, ""))
    }
  }

  describe("writeStringSchema()") {

    it("should convert an avro schema JSON file to all-string types and write to the dir specified") {
      import java.nio.file.Files
      val tempDir = Files.createTempDirectory("test-AvroSchemaStringifier-")
      val origSchemaFile = s"$tempDir/origSchema"
      val outputSchemaFile = s"$tempDir/outputSchema"

      writeFile(testAvroSchemaString, origSchemaFile)

      AvroSchemaStringifier.writeStringSchema(origSchemaFile, outputSchemaFile)

      import scala.io.Source
      val outputStr = Source.fromFile(outputSchemaFile).getLines.mkString

      val a = AvroSchema(outputStr)

      a.namespace should be ("ALS")
      a.`type` should be ("record")
      a.name should be ("impression")
      a.doc should be ("global documentation")
      a.fields.size should be (9)
      a.fields(0) should be (AvroSchemaField("StringField",       List("string"),         JString("0"), "StringFieldDoc"))
      a.fields(1) should be (AvroSchemaField("IntField",          List("null", "string"), JString("1"), "IntFieldDoc"))
      a.fields(2) should be (AvroSchemaField("LongField",         List("null", "string"), JString("2"), ""))
      a.fields(3) should be (AvroSchemaField("DoubleField",       List(        "string"), JString("0.3"), ""))
      a.fields(4) should be (AvroSchemaField("FloatField",        List("null", "string"), JString("0.4"), ""))
      a.fields(5) should be (AvroSchemaField("BooleanField",      List("null", "string"), JString("false"), ""))
      a.fields(6) should be (AvroSchemaField("NullField",         List("null"          ), JNull, ""))
      a.fields(7) should be (AvroSchemaField("LongNullDefault",   List("null", "string"), JNull, ""))
      a.fields(8) should be (AvroSchemaField("StringNullDefault", List("null", "string"), JNull, ""))
    }
  }

}


