package com.cars.bigdata.turbocow

import com.cars.bigdata.turbocow.actions.checks.{CheckParams, EqualChecker}
import org.json4s.jackson.JsonMethods._
import FieldLocation._
class EqualCheckerSpec extends UnitSpec {

  val checker = new EqualChecker
  val checker2 = new EqualChecker(false)
  val checker3 = new EqualChecker(true)

  def doCheck1(inputJson: String, checker : EqualChecker): Boolean = {
    checker.performCheck(
      CheckParams("A", Some("B"), Option(Input), Option(Input)),
      parse(inputJson),
      Map.empty[String, String],
      new ActionContext
    )
  }

  def doCheck2(enrichedMap: Map[String, String], checker : EqualChecker): Boolean = {
    checker.performCheck(
      CheckParams("A", Some("B"), Option(Enriched), Option(Enriched)),
      parse("{}"),
      enrichedMap,
      new ActionContext
    )
  }

  // when caseSensitive is missing or Null in config
  describe("performCheck() on input record") {

    it("should return true if A and B are equal") {
      doCheck1("""{"A": "testVal", "B": "testVal"}""", checker) should be(true)
    }
    it("should return false if A and B are not equal") {
      doCheck1("""{"A": "testVal", "B": "testVal1"}""", checker) should be(false)
    }
    it("should return false if A and B are equal but caseSensitive") {
      doCheck1("""{"A": "testVal", "B": "TESTVAL"}""", checker) should be(false)
    }
    it("should return false if A exists but not B") {
      doCheck1("""{"A": "X"}""", checker) should be(false)
    }
    it("should return false if B exists but not A") {
      doCheck1("""{"B": "Y"}""", checker) should be(false)
    }
    it("should return true if A and B are null") {
      doCheck1("""{"A": null, "B": null}""", checker) should be(true)
    }
    it("should return false if A and B are not present") {
      doCheck1("""{}""", checker) should be(false)
    }
    it("should return false if A is null and B is not present") {
      doCheck1("""{"A": null}""", checker) should be(false)
    }
  }
  describe("performCheck() on enriched record") {

    it("should return true if A and B are equal") {
      val enriched: Map[String, String] = Map("A" -> "X", "B" -> "X")
      doCheck2(enriched, checker) should be(true)
    }
    it("should return false if A and B are not equal") {
      val enriched: Map[String, String] = Map("A" -> "X", "B" -> "Y")
      doCheck2(enriched, checker) should be(false)
    }
    it("should return false if A exists but not B") {
      val enriched: Map[String, String] = Map("A" -> "X")
      doCheck2(enriched, checker) should be(false)
    }
    it("should return false if B exists but not A") {
      val enriched: Map[String, String] = Map("B" -> "Y")
      doCheck2(enriched, checker) should be(false)
    }
    it("should return true if A and B are null") {
      val enriched: Map[String, String] = Map("A" -> null, "B" -> null)
      doCheck2(enriched,checker) should be(true)
    }
    it("should return true if A and B are not present") {
      doCheck2(Map.empty[String, String], checker) should be(false)
    }
    it("should return false if A is null and B is not present") {
      val enriched: Map[String, String] = Map("A" -> null)
      doCheck2(enriched,checker) should be(false)
    }
  }
  describe("performCheck() on constant") {
    it("should return true if A and A are compared") {
      checker.performCheck(
        CheckParams("A", Some("A"), Option(Constant), Option(Constant)),
        parse("{}"),
        Map.empty[String, String],
        new ActionContext
      ) should be(true)
    }
    it("should return false if A and B are compared") {
      checker.performCheck(
        CheckParams("A", Some("B"), Option(Constant), Option(Constant)),
        parse("{}"),
        Map.empty[String, String],
        new ActionContext
      ) should be(false)
    }
  }

  // when caseSensitive is given a false value manually
  describe("performCheck() on input record with caseSensitive false") {

    it("should return true if A and B are equal with caseSensitive false") {
      doCheck1("""{"A": "testVal", "B": "testVal"}""",checker2) should be(true)
    }
    it("should return false if A and B are not equal with caseSensitive false") {
      doCheck1("""{"A": "testVal", "B": "testVal1"}""",checker2) should be(false)
    }
    it("should return false if A and B are equal but caseSensitive false") {
      doCheck1("""{"A": "testVal", "B": "TESTVAL"}""", checker2) should be(true)
    }
    it("should return false if A exists but not B with caseSensitive false") {
      doCheck1("""{"A": "X"}""",checker2) should be(false)
    }
    it("should return false if B exists but not A with caseSensitive false") {
      doCheck1("""{"B": "Y"}""", checker2) should be(false)
    }
    it("should return true if A and B are null with caseSensitive false") {
      doCheck1("""{"A": null, "B": null}""", checker2) should be(true)
    }
    it("should return false if A and B are not present with caseSensitive false") {
      doCheck1("""{}""",checker2) should be(false)
    }
    it("should return false if A is null and B is not present with caseSensitive false") {
      doCheck1("""{"A": null}""",checker2) should be(false)
    }
  }
  describe("performCheck() on enriched record with caseSensitive false") {

    it("should return true if A and B are equal with caseSensitive false") {
      val enriched: Map[String, String] = Map("A" -> "X", "B" -> "X")
      doCheck2(enriched, checker2) should be(true)
    }
    it("should return false if A and B are not equal with caseSensitive false") {
      val enriched: Map[String, String] = Map("A" -> "X", "B" -> "Y")
      doCheck2(enriched, checker2) should be(false)
    }
    it("should return false if A exists but not B with caseSensitive false") {
      val enriched: Map[String, String] = Map("A" -> "X")
      doCheck2(enriched, checker2) should be(false)
    }
    it("should return false if B exists but not A with caseSensitive false") {
      val enriched: Map[String, String] = Map("B" -> "Y")
      doCheck2(enriched, checker2) should be(false)
    }
    it("should return true if A and B are null with caseSensitive false") {
      val enriched: Map[String, String] = Map("A" -> null, "B" -> null)
      doCheck2(enriched,checker2) should be(true)
    }
    it("should return true if A and B are not present with caseSensitive false") {
      doCheck2(Map.empty[String, String], checker2) should be(false)
    }
    it("should return false if A is null and B is not present with caseSensitive false") {
      val enriched: Map[String, String] = Map("A" -> null)
      doCheck2(enriched,checker2) should be(false)
    }
  }
  describe("performCheck() on constant with caseSensitive false") {

    it("should return true if A and A are compared with caseSensitive false") {
      checker2.performCheck(
        CheckParams("A", Some("A"), Option(Constant), Option(Constant)),
        parse("{}"),
        Map.empty[String, String],
        new ActionContext
      ) should be(true)
    }
    it("should return false if A and B are compared with caseSensitive false") {
      checker2.performCheck(
        CheckParams("A", Some("B"), Option(Constant), Option(Constant)),
        parse("{}"),
        Map.empty[String, String],
        new ActionContext
      ) should be(false)
    }
  }

// when caseSensitive is given a default value manually
  describe("performCheck() on input record with caseSensitive true in specific: overrirde default") {

    it("should return true if A and B are equal with caseSensitive true in specific: overrirde default") {
      doCheck1("""{"A": "testVal", "B": "testVal"}""",checker3) should be(true)
    }
    it("should return false if A and B are not equal with caseSensitive true in specific: overrirde default") {
      doCheck1("""{"A": "testVal", "B": "testVal1"}""",checker3) should be(false)
    }
    it("should return false if A exists but not B with caseSensitive true in specific: overrirde default") {
      doCheck1("""{"A": "X"}""",checker3) should be(false)
    }
    it("should return false if B exists but not A with caseSensitive true in specific: overrirde default") {
      doCheck1("""{"B": "Y"}""", checker3) should be(false)
    }
    it("should return true if A and B are null with caseSensitive true in specific: overrirde default") {
      doCheck1("""{"A": null, "B": null}""", checker3) should be(true)
    }
    it("should return false if A and B are not present with caseSensitive true in specific: overrirde default") {
      doCheck1("""{}""",checker3) should be(false)
    }
    it("should return false if A is null and B is not present with caseSensitive true in specific: overrirde default") {
      doCheck1("""{"A": null}""",checker3) should be(false)
    }
  }
  describe("performCheck() on enriched record") {

    it("should return true if A and B are equal with caseSensitive true in specific: overrirde default") {
      val enriched: Map[String, String] = Map("A" -> "X", "B" -> "X")
      doCheck2(enriched, checker3) should be(true)
    }
    it("should return false if A and B are not equal with caseSensitive true in specific: overrirde default") {
      val enriched: Map[String, String] = Map("A" -> "X", "B" -> "Y")
      doCheck2(enriched, checker3) should be(false)
    }
    it("should return false if A exists but not B with caseSensitive true in specific: overrirde default") {
      val enriched: Map[String, String] = Map("A" -> "X")
      doCheck2(enriched, checker3) should be(false)
    }
    it("should return false if B exists but not A with caseSensitive true in specific: overrirde default") {
      val enriched: Map[String, String] = Map("B" -> "Y")
      doCheck2(enriched, checker3) should be(false)
    }
    it("should return true if A and B are null with caseSensitive true in specific: overrirde default") {
      val enriched: Map[String, String] = Map("A" -> null, "B" -> null)
      doCheck2(enriched,checker3) should be(true)
    }
    it("should return true if A and B are not present with caseSensitive true in specific: overrirde default") {
      doCheck2(Map.empty[String, String], checker3) should be(false)
    }
    it("should return false if A is null and B is not present with caseSensitive true in specific: overrirde default") {
      val enriched: Map[String, String] = Map("A" -> null)
      doCheck2(enriched,checker3) should be(false)
    }
  }
  describe("performCheck() on constant") {

    it("should return true if A and A are compared with caseSensitive true in specific: overrirde default") {
      checker3.performCheck(
        CheckParams("A", Some("A"), Option(Constant), Option(Constant)),
        parse("{}"),
        Map.empty[String, String],
        new ActionContext
      ) should be(true)
    }
    it("should return false if A and B are compared with caseSensitive true in specific: overrirde default") {
      checker3.performCheck(
        CheckParams("A", Some("B"), Option(Input), Option(Constant)),
        parse("""{ "A" : "b"}"""),
        Map.empty[String, String],
        new ActionContext
      ) should be(false)
    }
  }
}



