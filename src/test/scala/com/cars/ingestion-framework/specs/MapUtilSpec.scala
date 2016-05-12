package com.cars.ingestionframework

import org.scalatest.junit.JUnitRunner
import com.cars.ingestionframework.actions._

// Fix for Scalatest on Gradle:  (from http://stackoverflow.com/questions/18823855/cant-run-scalatest-with-gradle)
// Alternately, try using https://github.com/maiflai/gradle-scalatest
//@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class MapUtilSpec extends UnitSpec {

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
    //myAfterEach()
    super.afterEach()
  }

  // after all tests have run
  override def afterAll() = {
    super.afterAll()
  }

  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  // Tests start
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  describe("MapUtil.merge")  // ------------------------------------------------
  {
    it("should successfully merge two different maps") {

      val a = Map("A"->"AA", "B"->"BB")
      val b = Map("C"->"CC", "D"->"DD")

      MapUtil.merge(a, b) should be (Map("A"->"AA", "B"->"BB", "C"->"CC", "D"->"DD"))
    }

    it("should successfully merge a map with a duplicate key") {

      val a = Map("A"->"AA", "B"->"BB")
      val b = Map("B"->"BB", "C"->"CC")

      MapUtil.merge(a, b) should be (Map("A"->"AA", "B"->"BB", "C"->"CC"))
    }

    it("should successfully merge a map with a duplicated key but different value") {

      val a = Map("A"->"AA", "B"->"BB")
      val b = Map("B"->"XX", "C"->"CC")

      b should be (Map("B"->"XX", "C"->"CC"))
      MapUtil.merge(a, b) should be (Map("A"->"AA", "B"->"BB", "C"->"CC"))
    }
  }
}




