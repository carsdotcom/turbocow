package com.cars.bigdata.turbocow

import com.cars.bigdata.turbocow.actions._
import org.json4s.jackson.JsonMethods._

class InverseCheckerSpec extends UnitSpec {

  val checker = new InverseChecker(new EmptyChecker)

  describe("InverseChecker (with EmptyChecker inside)") {

    def doCheck(inputJson: String): Boolean = {

      checker.performCheck(
        CheckParams("fieldA"),
        parse(inputJson),
        Map.empty[String, String],
        new ActionContext
      )
    }

    it("should not return true if field is empty") {
      doCheck("""{"fieldA": ""}""") should be (!true)
    }
    it("should not return true if field is null") {
      doCheck("""{"fieldA": null}""") should be (!true)
    }
    it("should not return true if field is nonexistent") {
      doCheck("""{"X": ""}""") should be (!true)
    }

    it("should not return false if anything is in the field") {
      doCheck("""{"fieldA": "X"}""") should be (!false)
    }

  }

}




