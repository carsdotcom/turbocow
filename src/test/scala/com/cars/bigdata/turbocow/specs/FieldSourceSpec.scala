package com.cars.bigdata.turbocow

import scala.io.Source
import scala.util.Try
import org.json4s._
//import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

class FieldSourceSpec
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

  val defLoc = EnrichedThenInput

  describe("json constructor") {

    it("should default to EnrichedThenInput if not specified") {
      FieldSource.parseJVal(parse(""" "fieldname" """), Some(defLoc)) should be (FieldSource("fieldname", EnrichedThenInput))
    }

    it("should correctly convert explicit FieldLocations as specified") {
      FieldSource.parseJVal(parse(""" "$input.fieldname" """)) should be (FieldSource("fieldname", Input))
      FieldSource.parseJVal(parse(""" "$enriched.fieldname" """)) should be (FieldSource("fieldname", Enriched))
      FieldSource.parseJVal(parse(""" "$scratchpad.fieldname" """)) should be (FieldSource("fieldname", Scratchpad))
      FieldSource.parseJVal(parse(""" "$enriched-then-input.fieldname" """)) should be (FieldSource("fieldname", EnrichedThenInput))

      // test spaces
      FieldSource.parseJVal(parse(""" " $scratchpad.fieldname" """)) should be (FieldSource("fieldname", Scratchpad))
      FieldSource.parseJVal(parse(""" " $scratchpad.fieldname" """)) should be (FieldSource("fieldname", Scratchpad))
      FieldSource.parseJVal(parse(""" "$scratchpad.fieldname " """)) should be (FieldSource("fieldname", Scratchpad))
      FieldSource.parseJVal(parse(""" "$scratchpad.fieldname " """)) should be (FieldSource("fieldname", Scratchpad))
      FieldSource.parseJVal(parse(""" "$scratchpad .fieldname " """)) should be (FieldSource("fieldname", Scratchpad))
      FieldSource.parseJVal(parse(""" "$scratchpad. fieldname " """)) should be (FieldSource("fieldname", Scratchpad))
    }

    it("should throw if an unexpected FieldLocation is specified") {
      Try{ FieldSource.parseJVal(parse(""" "$ input.fieldname" """)) }.isSuccess should be (false)
      Try{ FieldSource.parseJVal(parse(""" "$inputt.fieldname" """)) }.isSuccess should be (false)
      Try{ FieldSource.parseJVal(parse(""" "$inpu.fieldname" """)) }.isSuccess should be (false)
      Try{ FieldSource.parseJVal(parse(""" "$nput.fieldname" """)) }.isSuccess should be (false)
      Try{ FieldSource.parseJVal(parse(""" "$.fieldname" """)) }.isSuccess should be (false)
      Try{ FieldSource.parseJVal(parse(""" "$ .fieldname" """)) }.isSuccess should be (false)
      Try{ FieldSource.parseJVal(parse(""" "$. fieldname" """)) }.isSuccess should be (false)
    }
  }

  describe("parseList") {

    it("should return empty list from empty array") {
      FieldSource.parseJArray(parse("""[ ]"""), Some(defLoc)) should be (List.empty[FieldSource])
    }

    it("should return same number of elements as input") {
      FieldSource.parseJArray(parse("""[ "$input.fieldA", "$enriched.fieldB" ]"""), Some(defLoc)) should
        be (List(FieldSource("fieldA", Input), FieldSource("fieldB", Enriched)))
    }

    it("should throw if one input element is bad") {
      Try{ FieldSource.parseJArray(parse("""[ "$input.fieldA", "$enrichedX.fieldB" ]"""), Some(defLoc)) }.isSuccess should be (false)
    }

    it("should throw if not passed a list") {
      Try{ FieldSource.parseJArray(parse(""" "X" """), Some(defLoc)) }.isSuccess should be (false)
      Try{ FieldSource.parseJArray(parse(""" null """), Some(defLoc)) }.isSuccess should be (false)
      Try{ FieldSource.parseJArray(parse(""" {"a":"b"} """), Some(defLoc)) }.isSuccess should be (false)
      Try{ FieldSource.parseJArray(parse(""" 9 """), Some(defLoc)) }.isSuccess should be (false)
      Try{ FieldSource.parseJArray(parse(""" 9.3 """), Some(defLoc)) }.isSuccess should be (false)
    }

  }

  describe("getValue()") {

    it("should return a value from the constant properly") {
      FieldSource("A", Constant).getValue(
        inputRecord = parse("""{"A": "AInput"}"""),
        currentEnrichedMap = Map("A"-> "AEnriched"),
        scratchPad = { val sp = new ScratchPad; sp.set("A", "AScratch"); sp }
      ) should be (Some("A"))
    }

    it("should return a value from the Input record") {
      FieldSource("A", Input).getValue(
        inputRecord = parse("""{"A": "AInput"}"""),
        currentEnrichedMap = Map("A"-> "AEnriched"),
        scratchPad = { val sp = new ScratchPad; sp.set("A", "AScratch"); sp }
      ) should be (Some("AInput"))
    }

    it("should return a value from the Enriched record") {
      FieldSource("A", Enriched).getValue(
        inputRecord = parse("""{"A": "AInput"}"""),
        currentEnrichedMap = Map("A"-> "AEnriched"),
        scratchPad = { val sp = new ScratchPad; sp.set("A", "AScratch"); sp }
      ) should be (Some("AEnriched"))
    }

    it("should return a value from the Scratchpad") {
      FieldSource("A", Scratchpad).getValue(
        inputRecord = parse("""{"A": "AInput"}"""),
        currentEnrichedMap = Map("A"-> "AEnriched"),
        scratchPad = { val sp = new ScratchPad; sp.set("A", "AScratch"); sp }
      ) should be (Some("AScratch"))
    }

    it("should return None if a value is missing in Input") {
      FieldSource("A", Input).getValue(
        inputRecord = parse("""{"AX": "AInput"}"""),
        currentEnrichedMap = Map("A"-> "AEnriched"),
        scratchPad = { val sp = new ScratchPad; sp.set("A", "AScratch"); sp }
      ) should be (None)
    }

    it("should return None if constant is null ") {
      FieldSource( null , Constant).getValue(
        inputRecord = parse("""{"AX": "AInput"}"""),
        currentEnrichedMap = Map("A"-> "AEnriched"),
        scratchPad = { val sp = new ScratchPad; sp.set("A", "AScratch"); sp }
      ) should be (None)
    }

    it("should return None if a value is null in Input") {
      FieldSource("A", Input).getValue(
        inputRecord = parse("""{"A": null}"""),
        currentEnrichedMap = Map("A"-> "AEnriched"),
        scratchPad = { val sp = new ScratchPad; sp.set("A", "AScratch"); sp }
      ) should be (None)
    }

    it("should return None if a value is missing in Enriched") {
      FieldSource("A", Enriched).getValue(
        inputRecord = parse("""{"A": "AInput"}"""),
        currentEnrichedMap = Map("AX"-> "AEnriched"),
        scratchPad = { val sp = new ScratchPad; sp.set("A", "AScratch"); sp }
      ) should be (None)
    }

    it("should return None if a value is null in Enriched") {
      FieldSource("A", Enriched).getValue(
        inputRecord = parse("""{"A": "AInput"}"""),
        currentEnrichedMap = Map("A"-> null),
        scratchPad = { val sp = new ScratchPad; sp.set("A", "AScratch"); sp }
      ) should be (None)
    }

    it("should return None if a value is missing in Scratchpad") {
      FieldSource("A", Scratchpad).getValue(
        inputRecord = parse("""{"A": "AInput"}"""),
        currentEnrichedMap = Map("A"-> "AEnriched"),
        scratchPad = { val sp = new ScratchPad; sp.set("AX", "AScratch"); sp }
      ) should be (None)
    }

    it("should return None if a value is null in Scratchpad") {
      FieldSource("A", Scratchpad).getValue(
        inputRecord = parse("""{"A": "AInput"}"""),
        currentEnrichedMap = Map("A"-> "AEnriched"),
        scratchPad = { val sp = new ScratchPad; sp.set("A", null); sp }
      ) should be (None)
    }
  }

  describe("isValueNull()") {

    it("should return false if constant is a String ") {
      FieldSource("A", Constant).isValueNull(
        inputRecord = parse("""{"A": null}"""),
        currentEnrichedMap = Map("A"-> "AEnriched"),
        scratchPad = { val sp = new ScratchPad; sp.set("A", "AScratch"); sp }
      ) should be (false)
    }

    it("should return true if constant is null") {
      FieldSource(null, Constant).isValueNull(
        inputRecord = parse("""{"A": null}"""),
        currentEnrichedMap = Map("A"-> "AEnriched"),
        scratchPad = { val sp = new ScratchPad; sp.set("A", "AScratch"); sp }
      ) should be (true)
    }

    it("should return true if a value is null in Input") {
      FieldSource("A", Input).isValueNull(
        inputRecord = parse("""{"A": null}"""),
        currentEnrichedMap = Map("A"-> "AEnriched"),
        scratchPad = { val sp = new ScratchPad; sp.set("A", "AScratch"); sp }
      ) should be (true)
    }

    it("should return false if a value is non-null in Input") {
      FieldSource("A", Input).isValueNull(
        inputRecord = parse("""{"A": "not null"}"""),
        currentEnrichedMap = Map("A"-> "AEnriched"),
        scratchPad = { val sp = new ScratchPad; sp.set("A", "AScratch"); sp }
      ) should be (false)
    }

    it("should return true if a value is null in Enriched") {
      FieldSource("A", Enriched).isValueNull(
        inputRecord = parse("""{"A": "AInput"}"""),
        currentEnrichedMap = Map("A"-> null),
        scratchPad = { val sp = new ScratchPad; sp.set("A", "AScratch"); sp }
      ) should be (true)
    }


    it("should return false if a value is non-null in Enriched") {
      FieldSource("A", Enriched).isValueNull(
        inputRecord = parse("""{"A": "AInput"}"""),
        currentEnrichedMap = Map("A"-> "not-null"),
        scratchPad = { val sp = new ScratchPad; sp.set("A", "AScratch"); sp }
      ) should be (false)
    }

    it("should return true if a value is null in Scratchpad") {
      FieldSource("A", Scratchpad).isValueNull(
        inputRecord = parse("""{"A": "AInput"}"""),
        currentEnrichedMap = Map("A"-> "AEnriched"),
        scratchPad = { val sp = new ScratchPad; sp.set("A", null); sp }
      ) should be (true)
    }

    it("should return false if a value is not-null in Scratchpad") {
      FieldSource("A", Scratchpad).isValueNull(
        inputRecord = parse("""{"A": "AInput"}"""),
        currentEnrichedMap = Map("A"-> "AEnriched"),
        scratchPad = { val sp = new ScratchPad; sp.set("A", "not-null"); sp }
      ) should be (false)
    }

    // enriched then input - if found, do not descend into input
    it("should return false if a value is non-null in Enriched for EnrichedThenInput") {
      FieldSource("A", EnrichedThenInput).isValueNull(
        inputRecord = parse("""{"A": "AInput"}"""),
        currentEnrichedMap = Map("A"-> "A"), // non-null
        scratchPad = { val sp = new ScratchPad; sp.set("A", "AScratch"); sp }
      ) should be (false)
    }
    it("should return true if a value is null in Enriched for EnrichedThenInput") {
      FieldSource("A", EnrichedThenInput).isValueNull(
        inputRecord = parse("""{"A": "AInput"}"""),
        currentEnrichedMap = Map("A"-> null),
        scratchPad = { val sp = new ScratchPad; sp.set("A", "AScratch"); sp }
      ) should be (true)
    }
    it("should return false if a value is nonexistent in Enriched & non-null in Input for EnrichedThenInput") {
      FieldSource("A", EnrichedThenInput).isValueNull(
        inputRecord = parse("""{"A": ""}"""), // non-null
        currentEnrichedMap = Map("X"-> "X"),
        scratchPad = { val sp = new ScratchPad; sp.set("A", "AScratch"); sp }
      ) should be (false)
    }
    it("should return true if a value is nonexistent in Enriched & null in Input for EnrichedThenInput") {
      FieldSource("A", EnrichedThenInput).isValueNull(
        inputRecord = parse("""{"A": null}"""),
        currentEnrichedMap = Map("X"-> "X"),
        scratchPad = { val sp = new ScratchPad; sp.set("A", "AScratch"); sp }
      ) should be (true)
    }
  }
}

 


