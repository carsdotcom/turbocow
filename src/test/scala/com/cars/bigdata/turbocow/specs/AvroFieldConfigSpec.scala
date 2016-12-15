package com.cars.bigdata.turbocow

import org.apache.spark.sql.types._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.Try

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

  describe("primary constructor") {

    val JNOTHING = parse("{}") \ "X"
    val JNULL = parse("null")

    it("should enforce having a default value") {

      // jnothing as the default will throw
      AvroFieldConfig.allSupportedAvroTypesMap.values.foreach{ theType =>
        println("theType = "+theType.toString)
        Try{ AvroFieldConfig(
          StructField("", theType, nullable=false), 
          defaultValue = JNOTHING
        )}.isSuccess should be (false)
      }

      // with a valid default value, we should succeed
      AvroFieldConfig.allSupportedAvroTypesMap.foreach{ case(typeStr, theType) =>
        Try{ AvroFieldConfig(
          StructField("", theType, nullable=false), 
          defaultValue = AvroFieldConfig.exampleJsonTypesMap(typeStr)
        )}.isSuccess should be (true)
      }

      // JNull should succeed but only if nullable == true
      AvroFieldConfig.allSupportedAvroTypesMap.values.foreach{ theType =>
        Try{ AvroFieldConfig(
          StructField("", theType, nullable=true), 
          defaultValue = JNULL
        )}.isSuccess should be (true)
      }
    }

    it("should error out if the default value is a JSON type incompatible with Avro type") {

      AvroFieldConfig.allSupportedAvroTypesMap.foreach{ case( typeStr, avroType ) =>
        
        // the 'good json' type should pass
        val goodJson = AvroFieldConfig.exampleJsonTypesMap(typeStr)
        Try{ AvroFieldConfig(
          StructField("", avroType, nullable=false), 
          goodJson
        )}.isSuccess should be (true)

        // the rest of the json types are bad and should fail
        println("good json = "+goodJson)
        val badJsons = AvroFieldConfig.exampleJsonTypesMapUniqueValues.filter( _ != goodJson )
        println("+++++++++++++++++++++++++++ badJsons = "+badJsons)
        badJsons.foreach{ badJson =>
          Try{ AvroFieldConfig(
            StructField("", avroType, nullable=false), 
            badJson
          )}.isSuccess should be (false)
        }
      }
    }
  }

  describe("apply()") {
    it("should create the correct default type") {

      // string ---------------------------------------------------
      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "null", "string" ],
        "default": null
      }""")) should be (
        AvroFieldConfig(
          StructField("n", StringType, nullable=true),
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
    }

  }

  describe("getDefaultValue()") {
    it("should return the right default value and type") {

      // string ---------------------------------------------------

      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "null", "string" ],
        "default": null
      }""")).getDefaultValue match { case null => ; case _ => fail() }

      AvroFieldConfig(parse("""{ 
        "name": "n",
        "type": [ "string" ],
        "default": "defString"
      }""")).getDefaultValue should be ("defString")

      // integral ---------------------------------------------------
      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "int" ],
        "default": 10
      }""")).getDefaultValue should be (10)

      // floating ---------------------------------------------------
      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "float" ],
        "default": 0.1
      }""")).getDefaultValue should be (0.1f)

      // boolean ---------------------------------------------------
      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "boolean" ],
        "default": true
      }""")).getDefaultValue match { case true => ; case _ => fail() }

      // null ---------------------------------------------------
      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "null" ],
        "default": null
      }""")).getDefaultValue match { case null => ; case _ => fail() }

      // this should throw - default config is required to getDefaultValue
      Try { AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "int" ]
      }""")).getDefaultValue }.isSuccess should be (false)

    }
  }

  describe("getDefaultValueAs()") {

    it("should return the right default value and type") {

      // string ---------------------------------------------------

      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "null", "string" ],
        "default": null
      }""")).getDefaultValueAs[String] match { case None => ; case _ => fail() }

      AvroFieldConfig(parse("""{ 
        "name": "n",
        "type": [ "string" ],
        "default": "defString"
      }""")).getDefaultValueAs[String] should be (Some("defString"))

      // integral ---------------------------------------------------
      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "int" ],
        "default": 10
      }""")).getDefaultValueAs[Int] should be (Some(10))

      // floating ---------------------------------------------------
      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "float" ],
        "default": 0.1
      }""")).getDefaultValueAs[Float] should be (Some(0.1f))

      // boolean ---------------------------------------------------
      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "boolean" ],
        "default": true
      }""")).getDefaultValueAs[Boolean] match { case Some(true) => ; case _ => fail() }

      // null ---------------------------------------------------
      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "null" ],
        "default": null
      }""")).getDefaultValueAs[Null] match { case None => ; case _ => fail() }

      // this should throw - default config is required to getDefaultValue
      Try { AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "int" ]
      }""")).getDefaultValueAs[String] }.isSuccess should be (false)
    }
  }

  describe("defaultToString()") {
    it("should convert all types properly") {

      // string ---------------------------------------------------
      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "null", "string" ],
        "default": null
      }""")).defaultToString should be (JNull)

      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "string" ],
        "default": "defString"
      }""")).defaultToString should be (JString("defString"))

      // integral ---------------------------------------------------
      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "int" ],
        "default": 10
      }""")).defaultToString should be (JString("10"))

      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "null", "int" ],
        "default": null
      }""")).defaultToString should be (JNull)

      // floating ---------------------------------------------------
      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "float" ],
        "default": 0.1
      }""")).defaultToString should be (JString("0.1"))

      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "null", "float" ],
        "default": null
      }""")).defaultToString should be (JNull)

      // boolean ---------------------------------------------------
      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "boolean" ],
        "default": true
      }""")).defaultToString should be (JString("true"))

      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "null", "boolean" ],
        "default": null
      }""")).defaultToString should be (JNull)

      // null ---------------------------------------------------
      AvroFieldConfig(parse("""{
        "name": "n",
        "type": [ "null" ],
        "default": null
      }""")).defaultToString should be (JNull)

    }
  }

}


