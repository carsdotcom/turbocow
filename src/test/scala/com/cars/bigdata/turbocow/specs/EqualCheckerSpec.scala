package com.cars.bigdata.turbocow.specs

import com.cars.bigdata.turbocow.{ActionContext, FieldSource, UnitSpec}
import com.cars.bigdata.turbocow.actions.checks.{CheckParams, EqualChecker}
import org.json4s.jackson.JsonMethods._

class EqualCheckerSpec extends UnitSpec {

  val checker = new EqualChecker

  describe("performCheck() on input record") {

    def doCheck(inputJson: String): Boolean = {

      checker.performCheck(
        CheckParams("A", Some("B"), Option(FieldSource.Input), Option(FieldSource.Input)),
        parse(inputJson),
        Map.empty[String, String],
        new ActionContext
      )
    }

    it("should return true if A and B are equal") {
      doCheck("""{"A": "testVal", "B": "testVal"}""") should be (true)
    }
    it("should return false if A and B are not equal") {
      doCheck("""{"A": "testVal", "B": "testVal1"}""") should be (false)
    }
    it("should return false if A exists but not B") {
      doCheck("""{"A": "X"}""") should be (false)
    }
    it("should return false if B exists but not A") {
      doCheck("""{"B": "Y"}""") should be (false)
    }
    it("should return true if A and B are null") {
      doCheck("""{"A": null, "B": null}""") should be (true)
    }
    it("should return false if A and B are not present") {
      doCheck("""{}""") should be (false)
    }
  }
}



