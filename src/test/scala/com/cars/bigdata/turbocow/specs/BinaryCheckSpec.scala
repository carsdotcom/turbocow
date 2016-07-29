package com.cars.bigdata.turbocow.specs

import com.cars.bigdata.turbocow.actions._
import com.cars.bigdata.turbocow.actions.checks._
import com.cars.bigdata.turbocow.{Action, ActionContext, ActionFactory, UnitSpec}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.Try

class BinaryCheckSpec extends UnitSpec {

  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  // Tests start
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  val resourcesDir = "./src/test/resources/"

  def makeNullAL = new ActionList(List(new NullAction))

  def getNullAction(action: Action): NullAction = action match {
    case na: NullAction => na
    case _ => fail()
  }

  describe("primary constructor") {

    it("should succeed if both onPass and onFail are specified") {
      val onPass = makeNullAL
      val onFail = makeNullAL
      val checker = new MockChecker

      val a = new BinaryCheck(
        left = "A",
        right = "B",
        checker = checker,
        onPass = onPass,
        onFail = onFail
      )
      a.left should be("A")
      a.right should be("B")
      a.checker should be(checker)
      a.onPass should be(onPass)
      a.onFail should be(onFail)
    }

    it("should throw if field/left is empty") {
      intercept[Exception](new BinaryCheck("", "B", new MockChecker))
    }

    it("should throw if field is null") {
      intercept[Exception](new BinaryCheck(null, "B", new MockChecker))
    }

    it("should throw if right is empty") {
      intercept[Exception](new BinaryCheck("A", "", new MockChecker))
    }

    it("should throw if right is null") {
      intercept[Exception](new BinaryCheck("A", null, new MockChecker))
    }

    it("should throw if checker is null") {
      intercept[Exception](new BinaryCheck("a", "b", null))
    }

    it("should throw if neither onPass or onFail is specified") {
      intercept[Exception](new BinaryCheck("a", "b", new MockChecker))
    }

    it("should not throw if onPass is there but onFail is missing") {
      Try(new BinaryCheck("a", "b", new MockChecker, onPass = makeNullAL)).isSuccess should be(true)
    }

    it("should not throw if onPass is missing but onFail is there") {
      Try(new BinaryCheck("a", "b", new MockChecker, onFail = makeNullAL)).isSuccess should be(true)
    }

    // todo add this test to ActionListSpec
    it("should throw if onPass or onFail contains a null Action") {
      intercept[Exception](new BinaryCheck("a", "b", new MockChecker, onPass = new ActionList(List(new NullAction, null))))
      intercept[Exception](new BinaryCheck("a", "b", new MockChecker, onFail = new ActionList(List(new NullAction, null))))
      intercept[Exception](new BinaryCheck("a", "b", new MockChecker, onPass = new ActionList(List(null, new NullAction))))
      intercept[Exception](new BinaryCheck("a", "b", new MockChecker, onFail = new ActionList(List(null, new NullAction))))
    }
  }

  describe("JSON constructor") {

    it("should construct - happy path") {
      val config = parse(
        s"""{
        "left": "A",
         "right": "B",
        "op": "equals",
        "onPass": [ 
          { 
            "actionType": "null",
            "config": { "name": "A PASS" }
          }
        ],
        "onFail": [ 
          { 
            "actionType": "null",
            "config": { "name": "A FAIL" }
          }
        ]
      }""")

      val a = new BinaryCheck(config, Some(new ActionFactory()))
      a.left should be("A")
      a.right should be("B")
      a.checker.isInstanceOf[EqualChecker] should be(true)
      a.onPass.actions.size should be(1)
      a.onFail.actions.size should be(1)
      val onPassAction = getNullAction(a.onPass.actions.head)
      val onFailAction = getNullAction(a.onFail.actions.head)
      onPassAction.name should be(Some("A PASS"))
      onFailAction.name should be(Some("A FAIL"))

    }

    it("should throw if left is missing") {
      val config = parse(
        s"""{
        "right": "B",
        "op": "equals",
        "onPass": [ 
          { 
            "actionType": "null",
            "config": { "name": "A PASS" }
          }
        ],
        "onFail": [ 
          { 
            "actionType": "null",
            "config": { "name": "A FAIL" }
          }
        ]
      }""")

      intercept[Exception](new BinaryCheck(config, Some(new ActionFactory())))
    }

    it("should throw if right is missing") {
      val config = parse(
        s"""{
        "left": "A",
        "op": "equals",
        "onPass": [
          {
            "actionType": "null",
            "config": { "name": "A PASS" }
          }
        ],
        "onFail": [
          {
            "actionType": "null",
            "config": { "name": "A FAIL" }
          }
        ]
      }""")

      intercept[Exception](new BinaryCheck(config, Some(new ActionFactory())))
    }

    it("should fail if op is missing") {
      val config = parse(
        s"""{
        "left": "A",
        "right": "B",
        "onFail": [ 
          { 
            "actionType": "null"
          }
        ]
      }""")

      intercept[Exception](new BinaryCheck(config, Some(new ActionFactory())))
    }

    it("should succeed if onPass is missing") {
      val config = parse(
        s"""{
        "left": "A",
        "right": "B",
        "op": "equals",
        "onFail": [ 
          { 
            "actionType": "null"
          }
        ]
      }""")

      Try(new BinaryCheck(config, Some(new ActionFactory()))).isSuccess should be(true)
    }

    it("should succeed if onFail is missing") {
      val config = parse(
        s"""{
        "left": "A",
        "right": "B",
        "op": "equals",
        "onPass": [ 
          { 
            "actionType": "null"
          }
        ]
      }""")

      Try(new BinaryCheck(config, Some(new ActionFactory()))).isSuccess should be(true)
    }

    it("should fail if both onPass & onFail are missing") {
      val config = parse(
        s"""{
        "left": "A",
        "right": "B",
        "op": "not-empty"
      }""")
      intercept[Exception](new BinaryCheck(config, Some(new ActionFactory())))
    }

    it("should fail if there is no config section at all") {
      val config = parse("""{"A":"A"}""") \ "jnothing"
      intercept[Exception](new BinaryCheck(config, Some(new ActionFactory())))
    }

  }

  describe("perform()") {

    val simpleConfig = parse(
      s"""{
        "left": "A",
        "right": "B",
        "op": "equals",
        "onPass": [ 
          { 
            "actionType": "null",
            "config": { "name": "A PASS" }
          }
        ],
        "onFail": [ 
          { 
            "actionType": "null",
            "config": { "name": "A FAIL" }
          }
        ]
      }""")

    it("should only perform actions in onPass when test passes") {
      val a = new BinaryCheck(simpleConfig, Some(new ActionFactory()))
      val onPassAction = getNullAction(a.onPass.actions.head)
      val onFailAction = getNullAction(a.onFail.actions.head)
      onPassAction.wasRun should be(false)
      onFailAction.wasRun should be(false)

      a.perform(parse("""{"A": "X", "B": "X"}"""), Map.empty[String, String], ActionContext())

      onPassAction.wasRun should be(true)
      onFailAction.wasRun should be(false)
    }

    it("should only perform actions in onFail when no right string present") {
      val a = new BinaryCheck(simpleConfig, Some(new ActionFactory()))
      val onPassAction = getNullAction(a.onPass.actions.head)
      val onFailAction = getNullAction(a.onFail.actions.head)
      onPassAction.wasRun should be(false)
      onFailAction.wasRun should be(false)

      a.perform(parse("""{"A": "x"}"""), Map.empty[String, String], ActionContext())

      onPassAction.wasRun should be(false)
      onFailAction.wasRun should be(true)
    }

    it("should only perform actions in onFail when no left string present") {
      val a = new BinaryCheck(simpleConfig, Some(new ActionFactory()))
      val onPassAction = getNullAction(a.onPass.actions.head)
      val onFailAction = getNullAction(a.onFail.actions.head)
      onPassAction.wasRun should be(false)
      onFailAction.wasRun should be(false)

      a.perform(parse("""{"B": "y"}"""), Map.empty[String, String], ActionContext())

      onPassAction.wasRun should be(false)
      onFailAction.wasRun should be(true)
    }

    it("should only perform actions in onFail when right is not equal to left") {
      val a = new BinaryCheck(simpleConfig, Some(new ActionFactory()))
      val onPassAction = getNullAction(a.onPass.actions.head)
      val onFailAction = getNullAction(a.onFail.actions.head)
      onPassAction.wasRun should be(false)
      onFailAction.wasRun should be(false)

      val result = a.perform(parse("""{"A": "x", "B": "y"}"""), Map.empty[String, String], ActionContext())

      onPassAction.wasRun should be(false)
      onFailAction.wasRun should be(true)
    }
  }

}


