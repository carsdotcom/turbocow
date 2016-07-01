package com.cars.turbocow

import org.scalatest.junit.JUnitRunner
import com.cars.turbocow.actions._

import scala.io.Source
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

import scala.util.{Try, Success, Failure}

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


class ItemSpec
  extends UnitSpec 
  //with MockitoSugar 
{
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

  describe("perform()") {
    it("should perform nothing if given an empty list") 
    {
      val inputRecord = parse("""{ "AField": "A", "BField": "B" }""")  
      val actionList = List.empty[Action]
      val sa = Item( actionList )
      val result = sa.perform(inputRecord, Map.empty[String, String], ActionContext())

      result.enrichedUpdates should be (Map.empty[String, String])
      result.stopProcessingActionList should be (false)
    }

    it("should perform one action if given one action") 
    {
      val inputRecord = parse("""{ "AField": "A", "BField": "B" }""")  
      val actionList = List( new AddEnrichedFields( List( ("key", "val") ) ) )
      val sa = Item( actionList )

      sa.actions.size should be (1)

      val result = sa.perform(inputRecord, Map.empty[String, String], ActionContext())

      result.enrichedUpdates should be (Map("key"->"val"))
      result.stopProcessingActionList should be (false)
    }
  }
}

  

