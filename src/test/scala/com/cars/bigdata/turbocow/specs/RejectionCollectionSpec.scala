package com.cars.bigdata.turbocow

import com.cars.bigdata.turbocow.actions._
import org.scalatest.junit.JUnitRunner

import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

class RejectionCollectionSpec 
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

  describe("RejectionCollection")  // ------------------------------------------------
  {
    it("should have no rejection reasons after construction") {
      val rc = new RejectionCollection
      rc.size should be (0)
    }

    it("should have one reason after adding one") {
      val rc = new RejectionCollection
      rc.size should be (0)
      rc.add("001")
      rc.size should be (1)
      rc.reasonList.head should be("001")
    }

    it("should have two reasons after adding two") {
      val rc = new RejectionCollection
      rc.size should be (0)
      rc.add("001")
      rc.add("002")
      rc.size should be (2)
      rc.reasonList(0) should be("001")
      rc.reasonList(1) should be("002")
    }

    it("should have no reasons after adding two and clearing") {
      val rc = new RejectionCollection
      rc.size should be (0)
      rc.add("001")
      rc.add("002")
      rc.size should be (2)
      rc.reasonList.size should be (2)
      rc.clear()
      rc.size should be (0)
      rc.reasonList.size should be (0)
    }

    it("should not modify the same reference that is given out in reasonsList") {
      val rc = new RejectionCollection
      rc.add("001")
      val list1 = rc.reasonList
      list1.size should be (1)

      rc.add("002")
      list1.size should be (1)
      rc.size should be (2)
      rc.reasonList.size should be (2)
    }
  }

  describe("RejectionCollection.toString") {
    it("should separate strings by '; '") {
      val rc = new RejectionCollection
      rc.add("001")
      rc.add("002")
      rc.toString should be ("001; 002")
    }

    it("should trim items when returning them") {
      val rc = new RejectionCollection
      rc.add("     001")
      rc.add("002     ")
      rc.add("    003     ")
      rc.toString should be ("001; 002; 003")
    }
  }

}
    
