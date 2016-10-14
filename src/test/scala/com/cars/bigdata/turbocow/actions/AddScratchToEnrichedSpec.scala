package com.cars.bigdata.turbocow.actions

import java.net.URI

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import com.cars.bigdata.turbocow._
import com.cars.bigdata.turbocow.test.SparkTestContext._

import scala.util.Try

/**
  * Created by rossstuart on 8/17/16.
  */
class AddScratchToEnrichedSpec
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
    //myAfterEach()
    super.afterEach()
  }

  // after all tests have run
  override def afterAll() = {
    super.afterAll()
  }


  describe("AddScratchedToEnriched and action engine"){
    it("should be able to enrich from the scratchpad when a test value is set") {
      val scratchPad: ScratchPad = new ScratchPad()
      scratchPad.set("jobRunTime","test123")
      scratchPad.set("applicationID", "applicationID_1234567890")
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration.json"),
        """{
          |  "activityType":"impressions",
          |  "items":[
          |    {
          |      "actions":[
          |        {
          |          "actionType":"add-scratch-to-enriched",
          |           "config": {
          |            "fields": ["applicationID", "jobRunTime"]
          |             }
          |         }
          |      ]
          |    }
          |  ]
          |}""".stripMargin, sc,
        None,
        new ActionFactory(new CustomActionCreator),
        scratchPad).collect()

      enriched.size should be (1) // always one because there's only one json input object
      enriched.head("jobRunTime") should be ("test123")
      enriched.head("applicationID") should be ("applicationID_1234567890")
      enriched.head.size should be (2 + 5) // 5 input records are copied too
    }

    it("should not add fields to enriched if they don't exist in the scratch pad") {
      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration.json"),
        """{
          |  "activityType":"impressions",
          |  "items":[
          |    {
          |      "actions":[
          |        {
          |          "actionType":"add-scratch-to-enriched",
          |           "config" : {
          |             "fields": ["applicationID", "jobRunTime"]
          |           }
          |        }
          |      ]
          |    }
          |  ]
          |}""".stripMargin, sc,
        None,
        new ActionFactory(new CustomActionCreator)).collect()

      enriched.head.get("applicationID") should be (None)
      enriched.head.get("jobRunTime") should be (None)
    }
  }
  describe("Primary constructor") {
   it("should throw if a key is empty") {
     Try{ new AddScratchToEnriched(List("")) }.isSuccess should be (false)
     }

    it("should throw if keyArray fields are empty") {
      Try{ new AddScratchToEnriched(List("test", "")) }.isSuccess should be (false)
      Try{ new AddScratchToEnriched(List("", "test")) }.isSuccess should be (false)
    }

    it("should throw exception if a key is an empty string") {
      Try{ new AddScratchToEnriched(List(" ")) }.isSuccess should be (false)
    }
    it("should throw if a Null List is passed") {
      Try{ new AddScratchToEnriched(null) }.isSuccess should be (false)
    }

    it("should throw if an empty List is passed") {
      Try{ new AddScratchToEnriched(Nil) }.isSuccess should be (false)
    }
  }


  describe("Secondary JSON consturctor") {
    it("Get the keys that are going to be in scratchPad from the JSON config") {
      val a = new AddScratchToEnriched(parse("""{"fields": ["applicationID", "jobRunTime"]}"""))
      a.keys should be (List("applicationID", "jobRunTime"))
    }

    it("should throw exception if a Jnothing is passed in") {
      val jNothing = parse("{}")\"x"
      Try{ new AddScratchToEnriched( jNothing )}.isSuccess should be (false)
    }

    it("should throw exception if fields is empty") {
      Try{ new AddScratchToEnriched(parse("""{"fields": [] }""")) }.isSuccess should be (false)
    }

    it("should throw is fields is not found") {
      Try{ new AddScratchToEnriched(parse("""{"fieldsX": ["test", "test123"] }""")) }.isSuccess should be (false)
    }

    it("should throw if the key is an empty string") {
      Try{ new AddScratchToEnriched(parse("""{"fields": ["test", ""] }""")) }.isSuccess should be (false)
      Try{ new AddScratchToEnriched(parse("""{"fields": ["", "test"] }""")) }.isSuccess should be (false)
      Try{ new AddScratchToEnriched(parse("""{"fields": ["", ""] }""")) }.isSuccess should be (false)
      Try{ new AddScratchToEnriched(parse("""{"fields": [""] }""")) }.isSuccess should be (false)
    }

    it("should throw if a key is all space") {
      Try{ new AddScratchToEnriched(parse("""{"fields": [" "] }""")) }.isSuccess should be (false)
    }

    it("should throw if the type of fields isn't an array") {
      Try{ new AddScratchToEnriched(parse("""{"fields": "string" }""")) }.isSuccess should be (false)
      Try{ new AddScratchToEnriched(parse("""{"fields": 0 }""")) }.isSuccess should be (false)
      Try{ new AddScratchToEnriched(parse("""{"fields": true }""")) }.isSuccess should be (false)
      Try{ new AddScratchToEnriched(parse("""{"fields": 0.1 }""")) }.isSuccess should be (false)
      Try{ new AddScratchToEnriched(parse("""{"fields": { "a": "b" }}""")) }.isSuccess should be (false)
    }

  }

  describe("getLookupRequirements") {

    it("should return default requirements") {

      // Null Action doesn't implement getLookupRequirements() so they should be
      // the same:
      val a = new AddScratchToEnriched(List("applicationID", "jobRunTime"))
      val nullAction = new NullAction()

      nullAction.getLookupRequirements should be (a.getLookupRequirements)
    }
  }
}
