package com.cars.bigdata.turbocow.specs

import com.cars.bigdata.turbocow.{ActionFactory, CachedLookupRequirement, UnitSpec}
import com.cars.bigdata.turbocow.actions._
import org.json4s.JObject
import org.json4s.jackson.JsonMethods._

import scala.util.Try

class GetLookupRequirementsSpec
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

  describe("getLookupRequirements()") {

    it("should return lookup requirements for lookup from onPass") {

      val config =s"""{
           |  "onPass" : [{
           |    "actionType" : "lookup",
           |    "config" : {
           |      "select" : ["xyz"],
           |      "fromDBTable" : "someTable",
           |      "where" : "keyField",
           |      "equals" : "AField"
           |    }
           |  }]
           |  }""".stripMargin
      val onFail = s""""""

      val actionList =  new ActionList(parse(config) \ "onPass", Option(actionFactory))
      val lookup = new Lookup(List("abcd"),"table",None, "hello" , "input", actionList)
      lookup.getLookupRequirements should be (
        List(CachedLookupRequirement("table", List("hello"),List("abcd"),None),
          CachedLookupRequirement("someTable",List("keyField"),List("xyz"),None)))
    }

    it("should return lookup requirements for lookup from onFail and onPass") {

      val config =s"""{
                      |  "onFail" : [{
                      |    "actionType" : "lookup",
                      |    "config" : {
                      |      "select" : ["xyz"],
                      |      "fromDBTable" : "someTable",
                      |      "where" : "keyField",
                      |      "equals" : "AField"
                      |    }
                      |  }],
                      |  "onPass" : [{
                      |    "actionType" : "lookup",
                      |    "config" : {
                      |      "select" : ["xyz2"],
                      |      "fromDBTable" : "someTable2",
                      |      "where" : "keyField2",
                      |      "equals" : "AField2"
                      |    }
                      |  }]
                      |  }""".stripMargin

      val onPass =  new ActionList(parse(config) \ "onPass", Option(actionFactory))
      val onFail =  new ActionList(parse(config) \ "onFail", Option(actionFactory))
      val lookup = new Lookup(List("abcd"),"table",None, "hello" , "input", onPass, onFail)
      lookup.getLookupRequirements should be (
        List(CachedLookupRequirement("table", List("hello"),List("abcd"),None),
          CachedLookupRequirement("someTable2",List("keyField2"),List("xyz2"),None),
          CachedLookupRequirement("someTable",List("keyField"),List("xyz"),None)
        ))
    }
  }
}

