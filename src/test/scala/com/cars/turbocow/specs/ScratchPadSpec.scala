package com.cars.turbocow

import com.cars.turbocow.actions._
import org.scalatest.junit.JUnitRunner

import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

class ScratchPadSpec 
  extends UnitSpec 
  with MockitoSugar {

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

  describe("ScratchPad")  { // ------------------------------------------------

    it("should have no elements after construction") {
      val sp = new ScratchPad()
      sp.size should be (0)
    }
  }

  describe("set") {
    it("should store something") {
      val sp = new ScratchPad()
      sp.set("A", "a some thing")
      sp.get("A") should be (Some("a some thing"))
      sp.size should be (1)
      sp.set("B", 123)
      sp.get("B") should be (Some(123))
      sp.size should be (2)
    }
  }

  describe("remove") {
    it("should remove the item") {
      val sp = new ScratchPad()
      sp.set("lookup", "AA")
      sp.size should be (1)
      sp.remove("lookup")
      sp.size should be (0)
      sp.get("lookup") should be (None)
    }
  }

  describe("setResult") {
    it("should save a result as a string") {
      val sp = new ScratchPad()
      sp.set("lookup-result", "XXX")
      sp.setResult("lookup", "result 1")
      sp.getResult("lookup") should be (Some("result 1"))
      sp.get("lookup-result") should be (Some("XXX"))
    }

    it("should save a result and not interfere with anything else") {
      val sp = new ScratchPad()
      sp.set("lookup-result", "XXX")
      sp.get("lookup-result") should be (Some("XXX"))

      sp.setResult("lookup", "result 1")
      sp.getResult("lookup") should be (Some("result 1"))
      sp.get("lookup-result") should be (Some("XXX"))
    }

    it("should overwrite previous results when setting a new result with same name") {
      val sp = new ScratchPad()
      sp.setResult("lookup", "result 1")
      sp.getResult("lookup") should be (Some("result 1"))

      sp.setResult("lookup", "XXX")
      sp.getResult("lookup") should be (Some("XXX"))
    }
  }

  describe("removeResult") {
    it("should remove the result") {
      val sp = new ScratchPad()
      sp.setResult("lookup", "result 1")
      sp.resultSize should be (1)
      sp.removeResult("lookup")
      sp.resultSize should be (0)
      sp.getResult("lookup") should be (None)
    }
  }

  describe("resultSize") {

    it("should return the right size") {
      val sp = new ScratchPad()
      sp.resultSize should be (0)

      sp.setResult("lookup", "result 1")
      sp.resultSize should be (1)
      sp.getResult("lookup") should be (Some("result 1"))
      sp.setResult("copy", "result 2")
      sp.resultSize should be (2)
      sp.getResult("copy") should be (Some("result 2"))
    }
  }

}
    

