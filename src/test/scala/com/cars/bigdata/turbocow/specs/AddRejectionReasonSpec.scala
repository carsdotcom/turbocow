package com.cars.bigdata.turbocow

import com.cars.bigdata.turbocow.actions._
import org.json4s.jackson.JsonMethods._

import scala.util.Try

class AddRejectionReasonSpec 
  extends UnitSpec 
{
  var actionFactory: ActionFactory = null

  // before all tests have run
  override def beforeAll() = {
    super.beforeAll()
  }

  // before each test has run
  override def beforeEach() = {
    super.beforeEach()
    actionFactory = new ActionFactory
  }

  // after each test has run
  override def afterEach() = {
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

  describe("constructor") {

    it("should throw if field is null") {

      Try{ new AddRejectionReason(field=null) }.isSuccess should be (false)
    }

    it("should throw if field is empty") {

      Try{ new AddRejectionReason("") }.isSuccess should be (false)
    }

    it("should succeed if field is nonempty") {

      Try{ new AddRejectionReason("X") }.isSuccess should be (true)
    }
  }

  describe("json constructor") {

    it("should throw if field is missing") {

      Try{ new AddRejectionReason(parse("""{"X":"X"}""")) }.isSuccess should be (false)
    }

    it("should throw if field is not a string") {

      Try{ new AddRejectionReason(parse("""{"field":null}""")) }.isSuccess should be (false)
      Try{ new AddRejectionReason(parse("""{"field":{"X":"X"}}""")) }.isSuccess should be (false)
      Try{ new AddRejectionReason(parse("""{"field":["X"]}""")) }.isSuccess should be (false)
      Try{ new AddRejectionReason(parse("""{"field":true}""")) }.isSuccess should be (false)
      Try{ new AddRejectionReason(parse("""{"field":1}""")) }.isSuccess should be (false)
      Try{ new AddRejectionReason(parse("""{"field":1.0}""")) }.isSuccess should be (false)
    }

    it("should throw if field is empty") {

      Try{ new AddRejectionReason(parse("""{"field":""}""")) }.isSuccess should be (false)
    }

    it("should succeed if field is nonempty") {

      Try{ new AddRejectionReason(parse("""{"field":"X"}""")) }.isSuccess should be (true)
    }
  }

  describe("perform()") {

    it("should add the specified field with all rejection reasons") 
    {

      val rejectReasons = List("reason A", "reason B")

      val record = parse("""{"K": "V"}""")

      val items = actionFactory.createItems(s"""{
        "activityType": "impressions",
        "items": [
          {
            "actions":[
              {
                "actionType":"reject",
                "config": {
                  "reason": "${rejectReasons(0)}"
                }
              }
            ]
          },
          {
            "actions":[
              {
                "actionType":"reject",
                "config": {
                  "reason": "${rejectReasons(1)}"
                }
              }
            ]
          },
          {
            "name": "final actions - MUST BE LAST!",
            "actions":[
              {
                "actionType":"add-rejection-reason",
                "config": {
                  "field": "reasonForRejectX"
                }
              }
            ]
          }
        ]
      }""".stripMargin)

      val enRecord: Map[String, String] = ActionEngine.processRecord(
        record,
        items)
    
      enRecord.size should be (2)

      // enriched record     
      enRecord.get("reasonForRejectX") should be (Some(rejectReasons.mkString("; ")))
      enRecord.get("K") should be (Some("V"))
    }

    it("should not add anything if no rejections were performed") {

      //todo
    }
  }

  describe("getLookupRequirements()") {

    it("should return default requirements") {

      // Null Action doesn't implement getLookupRequirements() so they should be
      // the same:
      val a = new AddRejectionReason("X")
      val nullAction = new NullAction()

      nullAction.getLookupRequirements should be (a.getLookupRequirements)
    }
  }
}

