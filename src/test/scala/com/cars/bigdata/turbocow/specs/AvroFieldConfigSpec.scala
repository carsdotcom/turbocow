package com.cars.bigdata.turbocow

import org.apache.spark.sql.types._
import org.json4s._
import org.json4s.jackson.JsonMethods._

class AvroFieldConfigSpec
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

  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  // Tests start
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  describe("apply()") {
    it("should create the correct default type") {

      // string ---------------------------------------------------
      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "string" ],
        "default": null
      }""")) should be (
        AvroFieldConfig(
          StructField("n", StringType, nullable=false),
          JNull
        )
      )

      AvroFieldConfig(parse("""{ 
        "name": "n",
        "type": [ "string" ],
        "default": "defString"
      }""")) should be (
        AvroFieldConfig(
          StructField("n", StringType, nullable=false),
          JString("defString")
        )
      )

      // integral ---------------------------------------------------
      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "int" ],
        "default": 10
      }""")) should be (
        AvroFieldConfig(
          StructField("n", IntegerType, nullable=false),
          JInt(10)
        )
      )

      // floating ---------------------------------------------------
      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "float" ],
        "default": 0.1
      }""")) should be (
        AvroFieldConfig(
          StructField("n", FloatType, nullable=false),
          JDouble(0.1)
        )
      )

      // boolean ---------------------------------------------------
      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "boolean" ],
        "default": true
      }""")) should be (
        AvroFieldConfig(
          StructField("n", BooleanType, nullable=false),
          JBool(true)
        )
      )

      // null ---------------------------------------------------
      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "null" ],
        "default": null
      }""")) should be (
        AvroFieldConfig(
          StructField("n", NullType, nullable=true),
          JNull
        )
      )

      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "null" ]
      }""")) should be (
        AvroFieldConfig(
          StructField("n", NullType, nullable=true),
          JNothing
        )
      )

      // missing default:
      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "null" ]
      }""")) should be (
        AvroFieldConfig(
          StructField("n", NullType, nullable=true),
          JNothing
        )
      )

    }
  }
    
}


