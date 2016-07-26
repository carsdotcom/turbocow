package com.cars.bigdata.turbocow

import com.cars.bigdata.turbocow.actions._
import org.json4s.jackson.JsonMethods._

class EmptyCheckerSpec extends UnitSpec {

  val checker = new EmptyChecker

  describe("performCheck() on input record") {

    def doCheck(inputJson: String): Boolean = {

      checker.performCheck(
        CheckParams("fieldA", leftSource=Option(FieldSource.Input)),
        parse(inputJson),
        Map.empty[String, String],
        new ActionContext
      )
    }

    it("should return true if field is empty") {
      doCheck("""{"fieldA": ""}""") should be (true)
    }
    it("should return true if field is null") {
      doCheck("""{"fieldA": null}""") should be (true)
    }
    it("should return true if field is nonexistent") {
      doCheck("""{"X": ""}""") should be (true)
    }
    it("should return false if anything is in the field") {
      doCheck("""{"fieldA": "X"}""") should be (false)
    }
  }

  describe("performCheck() on enriched record") {

    def doCheck(enrichedRecord: Map[String, String]): Boolean = {

      //println("enrichedRecord = "+enrichedRecord)
      checker.performCheck(
        CheckParams("fieldA", leftSource=Option(FieldSource.Enriched)),
        parse("""{"fieldA": "A"}"""),
        enrichedRecord,
        new ActionContext
      )
    }

    it("should return true if field is empty") {
      doCheck(Map("fieldA"->"")) should be (true)
    }
    it("should return true if field is null") {
      doCheck(Map("fieldA"->null)) should be (true)
    }
    it("should return true if field is nonexistent") {
      doCheck(Map("fieldX"->"")) should be (true)
    }
    it("should return false if anything is in the field") {
      doCheck(Map("fieldA"->"X")) should be (false)
    }
  }

}



