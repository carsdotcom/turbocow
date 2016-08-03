package com.cars.bigdata.turbocow

import com.cars.bigdata.turbocow.actions._

import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.io.Source
import scala.util.Try

class ReplaceNullSpec 
  extends UnitSpec 
{
  val testTable = "testtable"

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

  /** Helper fn
    */
  def fileToString(filePath: String) = Source.fromFile(filePath).getLines.mkString

  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  // Tests start
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  
  import FieldLocation._

  describe("primary (List of fields) constructor") {

    it("should throw if fields is null") {
      Try{ new ReplaceNull(null, "X", Input) }.isSuccess should be (false)
    }

    it("should throw if fields is empty") {
      Try{ new ReplaceNull(List.empty[FieldSource], "X", Input) }.isSuccess should be (false)
      Try{ new ReplaceNull(Nil, "X", Input) }.isSuccess should be (false)
    }

    it("should throw if newValue is null") {
      Try{ new ReplaceNull(List(FieldSource("X")), null, Input) }.isSuccess should be (false)
    }

    it("should throw if newValue is empty") {
      Try{ new ReplaceNull(List(FieldSource("X")), "", Input) }.isSuccess should be (false)
    }

    it("should throw if outputTo is null") {
      Try{ new ReplaceNull(List(FieldSource("X")), "newVal", null) }.isSuccess should be (false)
    }

    it("should succeed (happy path)") {
      Try{ new ReplaceNull(List(FieldSource("X")), "newVal", Input) }.isSuccess should be (true)
    }

    it("should default outputTo to Enriched if not specified") {
      (new ReplaceNull(List(FieldSource("X")), "newVal")).outputTo should be (FieldLocation.Enriched)
    }

  }

  describe("json constructor") {

    // happy
    it("should store the fields as provided (happy path)") {
      val a = new ReplaceNull(parse(s"""{
        "fields": ["A", "B"],
        "newValue": "X",
        "outputTo": "scratchpad"
      }"""))
      a.fields should be (List(FieldSource("A", EnrichedThenInput), FieldSource("B", EnrichedThenInput)))
      a.newValue should be ("X")
      a.outputTo should be (Scratchpad)
    }

    // field
    it("should parse one field properly (happy)") {
      val a = new ReplaceNull(parse(s"""{
        "field": "A",
        "newValue": "X",
        "outputTo": "scratchpad"
      }"""))
      a.fields should be (List(FieldSource("A", EnrichedThenInput)))
      a.newValue should be ("X")
      a.outputTo should be (Scratchpad)
    }
    it("should throw if field is null") {
      Try{ new ReplaceNull(parse(s"""{
        "field": null,
        "newValue": "X",
        "outputTo": "scratchpad"
      }""")) }.isSuccess should be (false)
    }
    it("should throw if field is empty") {
      Try{ new ReplaceNull(parse(s"""{
        "field": "",
        "newValue": "X",
        "outputTo": "scratchpad"
      }""")) }.isSuccess should be (false)
    }

    // fields
    it("should throw if no fields are provided") {
      Try { new ReplaceNull(parse("""{
        "newValue": "X",
        "outputTo": "scratchpad"
      }"""))
      }.isSuccess should be (false)
    }

    it("should throw if fields is null") {
      Try { new ReplaceNull(parse("""{
        "fields": null,
        "newValue": "X",
        "outputTo": "scratchpad"
      }"""))
      }.isSuccess should be (false)
    }

    it("should store the field as a one-item list if one field is given") {
      val a = new ReplaceNull(parse("""{
        "fields": ["A"],
        "newValue": "X",
        "outputTo": "scratchpad"
      }"""))
      a.fields should be (List(FieldSource("A")))
      a.newValue should be ("X")
      a.outputTo should be (Scratchpad)
    }

    //it("should be set to has invalid location modifier") {

    it("should throw if field has invalid location modifier") {
      Try{ new ReplaceNull(parse("""{
        "fields": ["$enrichedX.A"],
        "newValue": "X",
        "outputTo": "scratchpad"
      }""")) }.isSuccess should be (false)
    }

    // newValue
    it("should throw if no newValue is provided") {
      Try{ new ReplaceNull(parse("""{
        "fields": ["$enriched.A"],
        "outputTo": "scratchpad"
      }""")) }.isSuccess should be (false)
    }
    it("should throw if newValue is null") {
      Try{ new ReplaceNull(parse("""{
        "fields": ["$enriched.A"],
        "newValue": null,
        "outputTo": "scratchpad"
      }""")) }.isSuccess should be (false)
    }
    it("should throw if newValue is blank") {
      Try{ new ReplaceNull(parse("""{
        "fields": ["$enriched.A"],
        "newValue": "",
        "outputTo": "scratchpad"
      }""")) }.isSuccess should be (false)
    }

    // outputTo
    it("should throw if outputTo not specified") {
      Try{ new ReplaceNull(parse("""{
        "fields": ["A"],
        "newValue": "X"
      }""")) }.isSuccess should be (false)
    }
    it("should throw if outputTo null") {
      Try{ new ReplaceNull(parse("""{
        "fields": ["A"],
        "newValue": "X",
        "outputTo": null
      }""")) }.isSuccess should be (false)
    }
    it("should throw if outputTo is empty") {
      Try{ new ReplaceNull(parse("""{
        "fields": ["A"],
        "newValue": "X",
        "outputTo": ""
      }""")) }.isSuccess should be (false)
    }

    it("should set outputTo to Enriched if specified") {
      val a = new ReplaceNull(parse("""{
        "field": "A",
        "newValue": "X",
        "outputTo": "enriched"
      }"""))
      a.fields should be (List(FieldSource("A")))
      a.newValue should be ("X")
      a.outputTo should be (Enriched)
    }
    it("should set outputTo to Scratchpad if specified") {
      val a = new ReplaceNull(parse("""{
        "field": "A",
        "newValue": "X",
        "outputTo": "scratchpad"
      }"""))
      a.fields should be (List(FieldSource("A")))
      a.newValue should be ("X")
      a.outputTo should be (Scratchpad)
    }
    it("should throw if unrecognized outputTo") {
      Try{ new ReplaceNull(parse("""{
        "fields": ["A"],
        "newValue": "X",
        "outputTo": "scratchpadX"
      }""")) }.isSuccess should be (false)
    }

  }

  describe("getLookupRequirements") {

    it("should return default requirements") {

      // Null Action doesn't implement getLookupRequirements() so they should be
      // the same (the defaults):
      val a = new ReplaceNull(List(FieldSource("X")), "Value")
      val nullAction = new NullAction()

      nullAction.getLookupRequirements should be (a.getLookupRequirements)
    }
  }

  describe("perform") {
    it("should add to enriched record if specified") {

      val a = new ReplaceNull(List(FieldSource("A", Input)), "X", Enriched)
      a.perform(
        parse("""{ "A": null }"""),
        Map.empty[String, String],
        ActionContext()
      ) should be (PerformResult(Map("A"->"X")))
    }

    todo leftoff - add more tests
    
  }

  // old replace-null-with tests:
  //describe("replace null with") {
  //
  //  // Helper test function
  //  def testReplaceNull(value: String) = {
  //    println("value = "+value)
  //    val enriched: Array[Map[String, String]] = ActionEngine.processDir(
  //      new URI("./src/test/resources/input-integration-replacenullwith.json"),
  //      s"""
  //      {
  //        "activityType": "impressions",
  //        "items": [
  //          {
  //            "actions":[
  //              {
  //                "actionType": "replace-null-with-${value}",
  //                "config": {
  //                  "inputSource": [ "AField", "CField", "DField" ]
  //                }
  //              }
  //            ]
  //      
  //          }
  //        ]
  //      }""",
  //      sc).collect()
  //
  //    enriched.size should be (1) // always one because there's only one json input object
  //    //println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX enriched = "+enriched)
  //    enriched.head.size should be (2)
  //    enriched.head.get("CField") should be (Some(value)) // this one was null
  //    enriched.head.get("DField") should be (Some(value)) // this one was missing
  //    enriched.head.get("AField") should be (None) // do nothing to a field that is not null
  //    // note the semantics of this are weird.  todo - rethink this action
  //  }
  //
  //  it("should successfully process replace-null-with-X") {
  //    testReplaceNull("0")
  //    testReplaceNull("1")
  //    testReplaceNull("2")
  //    testReplaceNull("X")
  //    testReplaceNull("XXXXXYYYYZ have a nice day   ")
  //  }
  //
  //  // todo this could use more testing
  //}

}

 

