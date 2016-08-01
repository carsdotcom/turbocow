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

  describe("FieldSource constructor") {
    
    it("should default source to EnrichedThenInput if not specified") {

      FieldSource("X") should be (FieldSource("X", EnrichedThenInput))
    }
  }

  describe("json constructor") {

    it("should default to EnrichedThenInput if not specified") {
      FieldSource(parse(""" "fieldname" """)) should be (FieldSource("fieldname", EnrichedThenInput))
    }

    it("should correctly convert explicit FieldLocations as specified") {
      FieldSource(parse(""" "$input.fieldname" """)) should be (FieldSource("fieldname", Input))
      FieldSource(parse(""" "$enriched.fieldname" """)) should be (FieldSource("fieldname", Enriched))
      FieldSource(parse(""" "$scratchpad.fieldname" """)) should be (FieldSource("fieldname", Scratchpad))
      FieldSource(parse(""" "$enriched-then-input.fieldname" """)) should be (FieldSource("fieldname", EnrichedThenInput))

      // test spaces
      FieldSource(parse(""" " $scratchpad.fieldname" """)) should be (FieldSource("fieldname", Scratchpad))
      FieldSource(parse(""" " $scratchpad.fieldname" """)) should be (FieldSource("fieldname", Scratchpad))
      FieldSource(parse(""" "$scratchpad.fieldname " """)) should be (FieldSource("fieldname", Scratchpad))
      FieldSource(parse(""" "$scratchpad.fieldname " """)) should be (FieldSource("fieldname", Scratchpad))
      FieldSource(parse(""" "$scratchpad .fieldname " """)) should be (FieldSource("fieldname", Scratchpad))
      FieldSource(parse(""" "$scratchpad. fieldname " """)) should be (FieldSource("fieldname", Scratchpad))
    }

    it("should throw if an unexpected FieldLocation is specified") {
      Try{ FieldSource(parse(""" "$ input.fieldname" """)) }.isSuccess should be (false)
      Try{ FieldSource(parse(""" "$inputt.fieldname" """)) }.isSuccess should be (false)
      Try{ FieldSource(parse(""" "$inpu.fieldname" """)) }.isSuccess should be (false)
      Try{ FieldSource(parse(""" "$nput.fieldname" """)) }.isSuccess should be (false)
      Try{ FieldSource(parse(""" "$.fieldname" """)) }.isSuccess should be (false)
      Try{ FieldSource(parse(""" "$ .fieldname" """)) }.isSuccess should be (false)
      Try{ FieldSource(parse(""" "$. fieldname" """)) }.isSuccess should be (false)
    }
  }

  describe("parseList") {

    it("should return empty list from empty array") {
      FieldSource.parseList(parse("""[ ]""")) should be (List.empty[FieldSource])
    }

    it("should return same number of elements as input") {
      FieldSource.parseList(parse("""[ "$input.fieldA", "$enriched.fieldB" ]""")) should
        be (List(FieldSource("fieldA", Input), FieldSource("fieldB", Enriched)))
    }

    it("should throw if one input element is bad") {
      Try{ FieldSource.parseList(parse("""[ "$input.fieldA", "$enrichedX.fieldB" ]""")) }.isSuccess should be (false)
    }

    it("should throw if not passed a list") {
      Try{ FieldSource.parseList(parse(""" "X" """)) }.isSuccess should be (false)
      Try{ FieldSource.parseList(parse(""" null """)) }.isSuccess should be (false)
      Try{ FieldSource.parseList(parse(""" {"a":"b"} """)) }.isSuccess should be (false)
      Try{ FieldSource.parseList(parse(""" 9 """)) }.isSuccess should be (false)
      Try{ FieldSource.parseList(parse(""" 9.3 """)) }.isSuccess should be (false)
    }

  }


}

 


