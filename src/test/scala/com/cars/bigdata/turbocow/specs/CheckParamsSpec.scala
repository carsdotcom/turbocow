package com.cars.bigdata.turbocow

import com.cars.bigdata.turbocow.actions.checks.CheckParams
import org.json4s.jackson.JsonMethods._

class CheckParamsSpec extends UnitSpec {

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

  describe("getValueFrom()") {

    it("should return the valid enriched value if specified") {
      val cp = CheckParams(left = "A", leftSource = Option(FieldLocation.Enriched ))
      val input = parse("""{"A": "AInput"}""")
      val enriched = Map("A"->"AEnriched")
      cp.getValueFrom(Option(cp.left), cp.leftSource, input, enriched) should be (Some("AEnriched"))
    }
    it("should return None if enriched is specified but value not in enriched") {
      val cp = CheckParams(left = "A", leftSource = Option(FieldLocation.Enriched ))
      val input = parse("""{"A": "AInput"}""")
      val enriched = Map("XXX"->"XEnriched")
      cp.getValueFrom(Option(cp.left), cp.leftSource, input, enriched) should be (None)
    }

    it("should return the valid input value if specified") {
      val cp = CheckParams(left = "A", leftSource = Option(FieldLocation.Input ))
      val input = parse("""{"A": "AInput"}""")
      val enriched = Map("A"->"AEnriched")
      cp.getValueFrom(Option(cp.left), cp.leftSource, input, enriched) should be (Some("AInput"))
    }
    it("should return None if input is specified but value not in input") {
      val cp = CheckParams(left = "A", leftSource = Option(FieldLocation.Input ))
      val input = parse("""{"XA": "XAInput"}""")
      val enriched = Map("A"->"AEnriched")
      cp.getValueFrom(Option(cp.left), cp.leftSource, input, enriched) should be (None)
    }

    it("by default, should return the enriched value if enriched has a value") {
      val cp = CheckParams(left = "A")
      val input = parse("""{"A": "AInput"}""")
      val enriched = Map("A"->"AEnriched")
      cp.getValueFrom(Option(cp.left), cp.leftSource, input, enriched) should be (Some("AEnriched"))
    }
    it("by default, should return the input value if enriched does not have the value but input does") {
      val cp = CheckParams(left = "A")
      val input = parse("""{"A": "AInput"}""")
      val enriched = Map("XXA"->"XXAEnriched")
      cp.getValueFrom(Option(cp.left), cp.leftSource, input, enriched) should be (Some("AInput"))
    }
    it("by default, should return None if neither enriched nor input has the value") {
      val cp = CheckParams(left = "A")
      val input = parse("""{"XXA": "XXXAInput"}""")
      val enriched = Map("XXA"->"XXAEnriched")
      cp.getValueFrom(Option(cp.left), cp.leftSource, input, enriched) should be (None)
    }
    it("should return the field when field source is constant") {
      val cp = CheckParams(left = "A", leftSource = Option(FieldSource.Constant))
      val input = parse("""{"A": "AInput"}""")
      val enriched = Map("A"->"AEnriched")
      cp.getValueFrom(Option(cp.left), cp.leftSource, input, enriched) should be (Some("A"))
    }
  }

}




