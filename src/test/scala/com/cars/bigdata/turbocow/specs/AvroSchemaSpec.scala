package com.cars.bigdata.turbocow

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.{read, write}

import scala.io.Source


class AvroSchemaSpec extends UnitSpec {

  implicit val jsonFormats = org.json4s.DefaultFormats
                    
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
        "default": "X"
      }, {
        "name": "IntField",
        "type": [ "null", "int" ],
        "default": null
      }
    ]
  }"""
  
  describe("apply()") {

    it("should properly return an AvroSchema type") {

      val a: AvroSchema = AvroSchema(testAvroSchemaString)

      a.namespace should be ("ALS")
      a.`type` should be ("record")
      a.name should be ("impression")
      a.doc should be ("global documentation")
      a.fields should be (List(
        AvroSchemaField("StringField", List("string"), JString("X"), "StringFieldDoc"),
        AvroSchemaField("IntField", List("null", "int"), JNull, "")
      ))
    }
  }

  describe("toJson()") {

    it("should properly convert back to JSON") {

      val testAvroSchemaString = """{
        "namespace": "ALS",
        "type": "record",
        "name": "impression",
        "doc": "global documentation",
        "fields": [{
            "name": "StringField",
            "type": [ "string" ],
            "doc": "StringFieldDoc",
            "default": "X"
          }, {
            "name": "IntField",
            "type": [ "null", "int" ],
            "default": null
          }
        ]
      }"""
      
      val origAS: AvroSchema = AvroSchema(testAvroSchemaString)

      val rereadAS = AvroSchema(origAS.toJson)

      origAS should be (rereadAS)
    }
  }

}



