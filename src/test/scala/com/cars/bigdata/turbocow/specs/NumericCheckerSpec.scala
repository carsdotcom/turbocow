package com.cars.bigdata.turbocow.specs

import com.cars.bigdata.turbocow.{ActionContext, UnitSpec}
import com.cars.bigdata.turbocow.actions._
import com.cars.bigdata.turbocow.actions.checks.NumericChecker
import org.json4s.jackson.JsonMethods._

class NumericCheckerSpec extends UnitSpec {

  val checker = new NumericChecker

  describe("NumeircChecker") {

    def doCheck(inputJson: String): Boolean = {

      checker.performCheck(
        CheckParams("fieldA"),
        parse(inputJson),
        Map.empty[String, String],
        new ActionContext
      )
    }

    it("should return true if field is numeric") {
      doCheck("""{"fieldA": "597"}""") should be (true)
    }
    it("should return true if field is null") {
      doCheck("""{"fieldA": null}""") should be (false)
    }
    it("should return true if field is nonexistent") {
      doCheck("""{"X": ""}""") should be (false)
    }

    it("should return false if anything is in the field") {
      doCheck("""{"fieldA": "sjgfr3435"}""") should be (false)
    }

  }

}



