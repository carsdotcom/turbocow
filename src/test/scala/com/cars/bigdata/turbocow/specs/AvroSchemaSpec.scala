package com.cars.bigdata.turbocow

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.{read, write}

import scala.io.Source
import scala.util.Try


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
    "namespace": "namespace-of-this",
    "type": "record",
    "name": "name-of-this",
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
      }, {
        "name": "FloatField",
        "type": "float",
        "default": 10.1
      }
    ]
  }"""
  
  describe("apply()") {

    it("should properly return an AvroSchema type") {

      val a: AvroSchema = AvroSchema(testAvroSchemaString)

      a.namespace should be ("namespace-of-this")
      a.`type` should be ("record")
      a.name should be ("name-of-this")
      a.doc should be ("global documentation")

      a.fields(0) should be (AvroSchemaField("StringField", List("string"), JString("X"), doc="StringFieldDoc"))
      a.fields(1) should be (AvroSchemaField("IntField", List("null", "int"), JNull, doc=""))
      a.fields(2) should be (AvroSchemaField("FloatField", List("float"), JDouble(10.1), doc=""))
      a.fields.size should be (3)
    }

    it("should throw if the type field is empty or missing") {
      Try{ AvroSchema("""{
        "namespace": "namespace-of-this",
        "type": "record",
        "name": "name-of-this",
        "fields": [{
            "name": "StringField"
          }
        ]
      }""") }.isSuccess should be (false)

      Try{ AvroSchema("""{
        "namespace": "namespace-of-this",
        "type": "record",
        "name": "name-of-this",
        "fields": [{
            "name": "StringField",
            "type": ""
          }
        ]
      }""") }.isSuccess should be (false)

      Try{ AvroSchema("""{
        "namespace": "namespace-of-this",
        "type": "record",
        "name": "name-of-this",
        "fields": [{
            "name": "StringField",
            "type": " "
          }
        ]
      }""") }.isSuccess should be (false)
    }

    it("should throw if the default field is empty or missing") {
      Try{ AvroSchema("""{
        "namespace": "namespace-of-this",
        "type": "record",
        "name": "name-of-this",
        "fields": [{
            "name": "StringField",
            "type": "string"
          }
        ]
      }""") }.isSuccess should be (false)

      Try{ AvroSchema("""{
        "namespace": "namespace-of-this",
        "type": "record",
        "name": "name-of-this",
        "fields": [{
            "name": "IntField",
            "type": [ "null", "int" ]
          }
        ]
      }""") }.isSuccess should be (false)

      Try{ AvroSchema("""{
        "namespace": "namespace-of-this",
        "type": "record",
        "name": "name-of-this",
        "fields": [{
            "name": "IntField",
            "type": [ "null", "int" ],
            "default": null
          }
        ]
      }""") }.isSuccess should be (true)
    }

    it("should throw if the name field is empty or missing") {
      Try{ AvroSchema("""{
        "namespace": "namespace-of-this",
        "type": "record",
        "name": "name-of-this",
        "fields": [{
            "type": "string",
            "default": ""
          }
        ]
      }""") }.isSuccess should be (false)

      Try{ AvroSchema("""{
        "namespace": "namespace-of-this",
        "type": "record",
        "name": "name-of-this",
        "fields": [{
            "name": "",
            "type": [ "null", "int" ],
            "default": null
          }
        ]
      }""") }.isSuccess should be (false)

      Try{ AvroSchema("""{
        "namespace": "namespace-of-this",
        "type": "record",
        "name": "name-of-this",
        "fields": [{
            "name": "    ",
            "type": [ "null", "int" ],
            "default": null
          }
        ]
      }""") }.isSuccess should be (false)

      // the name field is trimmed so this should pass:
      val as = Try{ AvroSchema("""{
        "namespace": "namespace-of-this",
        "type": "record",
        "name": "name-of-this",
        "fields": [{
            "name": "  x  ",
            "type": [ "null", "int" ],
            "default": null
          }
        ]
      }""") }
      as.isSuccess should be (true)
      as.get.fields.head.name should be ("x")
    }
  }

  describe("toJson()") {

    it("should properly convert back to JSON") {

      val origAS: AvroSchema = AvroSchema(testAvroSchemaString)

      val rereadAS = AvroSchema(origAS.toJson)

      origAS should be (rereadAS)
    }
  }

}



