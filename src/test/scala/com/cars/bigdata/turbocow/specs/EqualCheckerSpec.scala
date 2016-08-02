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
    it("should return false if A is null and B is not present") {
      doCheck("""{"A": null}""") should be (false)
    }
  }
  describe("performCheck() on enriched record") {

    def doCheck(enrichedMap: Map[String, String]): Boolean = {

      checker.performCheck(
        CheckParams("A", Some("B"), Option(FieldSource.Enriched), Option(FieldSource.Enriched)),
        parse("{}"),
        enrichedMap,
        new ActionContext
      )
    }

    it("should return true if A and B are equal") {
      val enriched: Map[String, String] = Map("A"->"X", "B"->"X")
      doCheck(enriched) should be (true)
    }
    it("should return false if A and B are not equal") {
      val enriched: Map[String, String] = Map("A"->"X", "B"->"Y")
      doCheck(enriched) should be (false)
    }
    it("should return false if A exists but not B") {
      val enriched: Map[String, String] = Map("A"->"X")
      doCheck(enriched) should be (false)
    }
    it("should return false if B exists but not A") {
      val enriched: Map[String, String] = Map("B"->"Y")
      doCheck(enriched) should be (false)
    }
    it("should return true if A and B are null") {
      val enriched: Map[String, String] = Map("A"->null, "B"->null)
      doCheck(enriched) should be (true)
    }
    it("should return true if A and B are not present") {
      doCheck(Map.empty[String, String]) should be (true)
    }
    it("should return false if A is null and B is not present") {
      val enriched: Map[String, String] = Map("A"->null)
      doCheck(enriched) should be (false)
    }
  }
  describe("performCheck() on constant") {

    it("should return true if A and A are compared") {
      checker.performCheck(
        CheckParams("A", Some("A"), Option(FieldSource.Constant), Option(FieldSource.Constant)),
        parse("{}"),
        Map.empty[String, String],
        new ActionContext
      ) should be (true)
    }
    it("should return false if A and B are compared") {
      checker.performCheck(
        CheckParams("A", Some("B"), Option(FieldSource.Constant), Option(FieldSource.Constant)),
        parse("{}"),
        Map.empty[String, String],
        new ActionContext
      ) should be (false)
    }
  }
}



