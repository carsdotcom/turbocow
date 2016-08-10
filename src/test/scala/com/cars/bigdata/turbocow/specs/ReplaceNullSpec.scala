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

  val defLoc = ReplaceNull.defaultLocation

  describe("primary (List of fields) constructor") {

    it("should throw if fields is null") {
      Try{ new ReplaceNull(null, "X", Input) }.isSuccess should be (false)
    }

    it("should throw if fields is empty") {
      Try{ new ReplaceNull(List.empty[FieldSource], "X", Input) }.isSuccess should be (false)
      Try{ new ReplaceNull(Nil, "X", Input) }.isSuccess should be (false)
    }

    it("should throw if newValue is null") {
      Try{ new ReplaceNull(List(FieldSource("X", defLoc)), null, Input) }.isSuccess should be (false)
    }

    it("should throw if newValue is empty") {
      Try{ new ReplaceNull(List(FieldSource("X", defLoc)), "", Input) }.isSuccess should be (false)
    }

    it("should throw if outputTo is null") {
      Try{ new ReplaceNull(List(FieldSource("X", defLoc)), "newVal", null) }.isSuccess should be (false)
    }

    it("should succeed (happy path)") {
      Try{ new ReplaceNull(List(FieldSource("X", defLoc)), "newVal", Input) }.isSuccess should be (true)
    }

    it("should default outputTo to EnrichedThenInput if not specified") {
      (new ReplaceNull(List(FieldSource("X", defLoc)), "newVal")).outputTo should be (FieldLocation.EnrichedThenInput)
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
      a.fields should be (List(FieldSource("A", defLoc)))
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
    it("should default to Enriched if outputTo is missing") {
      val a = new ReplaceNull(parse(s"""{
        "fields": ["A", "B"],
        "newValue": "X"
      }"""))
      a.fields should be (List(FieldSource("A", EnrichedThenInput), FieldSource("B", EnrichedThenInput)))
      a.newValue should be ("X")
      a.outputTo should be (Enriched)
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
        "fields": ["A"],
        "newValue": "X",
        "outputTo": "enriched"
      }"""))
      a.fields should be (List(FieldSource("A", defLoc)))
      a.newValue should be ("X")
      a.outputTo should be (Enriched)
    }
    it("should set outputTo to Scratchpad if specified") {
      val a = new ReplaceNull(parse("""{
        "fields": ["A"],
        "newValue": "X",
        "outputTo": "scratchpad"
      }"""))
      a.fields should be (List(FieldSource("A", defLoc)))
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

    it("should create the same object if passing fields:[A] and field:A") {
      val listAction = new ReplaceNull(parse("""{
        "fields": ["A"],
        "newValue": "X",
        "outputTo": "scratchpad"
      }"""))
      // single-field action
      val sfAction = new ReplaceNull(parse("""{
        "field": "A",
        "newValue": "X",
        "outputTo": "scratchpad"
      }"""))

      // make sure fields list is same
      listAction.fields.size should be (1)
      listAction.fields.size should be (sfAction.fields.size)
      listAction.fields.head should be (FieldSource("A", EnrichedThenInput))
      listAction.fields.head should be (sfAction.fields.head)

      // just to be thorough:
      listAction.newValue should be (sfAction.newValue)
      listAction.outputTo should be (sfAction.outputTo)
    }
  }

  describe("getLookupRequirements") {

    it("should return default requirements") {

      // Null Action doesn't implement getLookupRequirements() so they should be
      // the same (the defaults):
      val a = new ReplaceNull(List(FieldSource("X", defLoc)), "Value")
      val nullAction = new NullAction()

      nullAction.getLookupRequirements should be (a.getLookupRequirements)
    }
  }

  describe("perform") {

    // test output locations
    it("should add to enriched record if specified") {

      val a = new ReplaceNull(List(FieldSource("A", Input)), "X", Enriched)
      val ac = ActionContext()
      a.perform(
        parse("""{ "A": null }"""),
        Map.empty[String, String],
        ac
      ) should be (PerformResult(Map("A"->"X")))

      // scratchpad should not have changed
      ac.scratchPad.allMainPad should be (ActionContext().scratchPad.allMainPad)
      ac.scratchPad.allResults should be (ActionContext().scratchPad.allResults)
    }

    it("should add nothing if input is not null (ie. string 'null')") {

      val a = new ReplaceNull(List(FieldSource("A", Input)), "X", Enriched)
      val ac = ActionContext()
      a.perform(
        parse("""{ "A": "null" }"""),  // note this is a string "null" (not null)
        Map.empty[String, String],
        ac
      ) should be (PerformResult())

      // scratchpad should not have changed
      ac.scratchPad.allMainPad should be (ActionContext().scratchPad.allMainPad)
      ac.scratchPad.allResults should be (ActionContext().scratchPad.allResults)
    }

    it("should add to scratchpad record if specified") {

      val a = new ReplaceNull(List(FieldSource("A", Input)), "X", Scratchpad)
      val ac = ActionContext()
      a.perform(
        parse("""{ "A": null }"""),
        Map.empty[String, String],
        ac
      ) should be (PerformResult())

      // scratchpad should be updated
      ac.scratchPad.allMainPad.size should be (1)
      ac.scratchPad.get("A") should be (Some("X"))
      // scratchpad results should not have changed
      ac.scratchPad.allResults should be (ActionContext().scratchPad.allResults)
    }

    it("should add nothing to scratchpad record if not null") {

      val a = new ReplaceNull(List(FieldSource("A", Input)), "X", Scratchpad)
      val ac = ActionContext()
      a.perform(
        parse("""{ "A": "" }"""), // blank, not null
        Map.empty[String, String],
        ac
      ) should be (PerformResult())

      // scratchpad should not have changed
      ac.scratchPad.allMainPad should be (ActionContext().scratchPad.allMainPad)
      ac.scratchPad.allResults should be (ActionContext().scratchPad.allResults)
    }

    // test input locations: enriched
    it("should add to enriched record if enriched is null") {

      val a = new ReplaceNull(List(FieldSource("A", Enriched)), "X", Enriched)
      val ac = ActionContext()
      a.perform(
        parse("""{ "A": "A" }"""),
        Map("A"->null),
        ac
      ) should be (PerformResult(Map("A"->"X")))
    }

    it("should not add to enriched record if enriched is non-null") {

      val a = new ReplaceNull(List(FieldSource("A", Enriched)), "X", Enriched)
      val ac = ActionContext()
      a.perform(
        parse("""{ "A": "A" }"""),
        Map("A"->""),// not null, empty string
        ac
      ) should be (PerformResult())
    }

    // test input locations: scratch
    it("should add to enriched record if scratchpad is null") {

      val a = new ReplaceNull(List(FieldSource("A", Scratchpad)), "X", Enriched)
      val sc = new ScratchPad()
      sc.set("A", null)
      val ac = ActionContext(scratchPad = sc)

      a.perform(
        parse("""{ "A": "A" }"""),
        Map.empty[String, String],
        ac
      ) should be (PerformResult(Map("A"->"X")))
    }

    it("should not add to enriched record if scratchpad is not null") {

      val a = new ReplaceNull(List(FieldSource("A", Scratchpad)), "X", Enriched)
      val sc = new ScratchPad()
      sc.set("A", "") // not null
      val ac = ActionContext(scratchPad = sc)

      a.perform(
        parse("""{ "A": "A" }"""),
        Map.empty[String, String],
        ac
      ) should be (PerformResult())
    }

    // test input locations: EnrichedThenInput
    it("should add to enriched record for EnrichedThenInput if Enriched is null") {

      val a = new ReplaceNull(List(FieldSource("A", EnrichedThenInput)), "X", Enriched)
      val ac = ActionContext()
      a.perform(
        parse("""{ "A": "A" }"""),
        Map("A"->null),
        ac
      ) should be (PerformResult(Map("A"->"X")))
    }

    it("should add to enriched record for EnrichedThenInput if Enriched is nonexistent and Input is null") {

      val a = new ReplaceNull(List(FieldSource("A", EnrichedThenInput)), "X", Enriched)
      val ac = ActionContext()
      a.perform(
        parse("""{ "A": null }"""),
        Map("X"->"X"),
        ac
      ) should be (PerformResult(Map("A"->"X")))
    }

    it("should NOT add to enriched record for EnrichedThenInput if Enriched is nonexistent and Input is not null") {

      val a = new ReplaceNull(List(FieldSource("A", EnrichedThenInput)), "X", Enriched)
      val ac = ActionContext()
      a.perform(
        parse("""{ "A": "" }"""), // not null
        Map("X"->"X"),
        ac
      ) should be (PerformResult())
    }

  }
}       


 

