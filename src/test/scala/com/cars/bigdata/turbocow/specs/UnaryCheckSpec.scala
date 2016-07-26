package com.cars.bigdata.turbocow

import com.cars.bigdata.turbocow.actions._
import com.cars.bigdata.turbocow.test.SparkTestContext._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.Try

class UnaryCheckSpec extends UnitSpec {

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

      val a = new UnaryCheck(
        field = "A", 
        checker = checker,
        onPass = onPass,
        onFail = onFail
      )
      a.field should be ("A")
      a.checker should be (checker)
      a.onPass should be (onPass)
      a.onFail should be (onFail)
    }

    it("should throw if field is empty") {
      intercept[Exception]( new UnaryCheck("", new MockChecker) )
    }

    it("should throw if field is null") {
      intercept[Exception]( new UnaryCheck(null, new MockChecker) )
    }

    it("should throw if checker is null") {
      intercept[Exception]( new UnaryCheck("a", null) )
    }

    it("should throw if neither onPass or onFail is specified") {
      intercept[Exception]( new UnaryCheck("field", new MockChecker) )
    }

    it("should not throw if onPass is there but onFail is missing") {
      Try( new UnaryCheck("field", new MockChecker, onPass=makeNullAL) ).isSuccess should be (true)
    }

    it("should not throw if onPass is missing but onFail is there") {
      Try( new UnaryCheck("field", new MockChecker, onFail=makeNullAL) ).isSuccess should be (true)
    }

    // todo add this test to ActionListSpec
    it("should throw if onPass or onFail contains a null Action") {
      intercept[Exception]( new UnaryCheck("fn", new MockChecker, onPass=new ActionList(List(new NullAction, null))))
      intercept[Exception]( new UnaryCheck("fn", new MockChecker, onFail=new ActionList(List(new NullAction, null))))
      intercept[Exception]( new UnaryCheck("fn", new MockChecker, onPass=new ActionList(List(null, new NullAction))))
      intercept[Exception]( new UnaryCheck("fn", new MockChecker, onFail=new ActionList(List(null, new NullAction))))
    }
  }

  describe("JSON constructor") {

    it("should construct - happy path") {
      val config = parse(s"""{
        "field": "A",
        "op": "not-empty",
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

      val a = new UnaryCheck(config, Some(new ActionFactory()))
      a.field should be ("A")
      a.checker match {
        case c: InverseChecker => c.checker match {
          case c: EmptyChecker => ;
          case _ => fail()
        }
        case _ => fail()
      }
      a.onPass.actions.size should be (1)
      a.onFail.actions.size should be (1)
      val onPassAction = getNullAction(a.onPass.actions.head)
      val onFailAction = getNullAction(a.onFail.actions.head)
      onPassAction.name should be (Some("A PASS"))
      onFailAction.name should be (Some("A FAIL"))

      // Note, this fails too:
      //List[Action](new NullAction(Some("A"))) should be (List[Action](new NullAction(Some("A"))))
    }

    it("should throw if field is missing") {
      val config = parse(s"""{
        "op": "not-empty",
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

      intercept[Exception]( new UnaryCheck(config, Some(new ActionFactory())) )
    }

    it("should fail if op is missing") {
      val config = parse(s"""{
        "field": "A",
        "onFail": [ 
          { 
            "actionType": "null"
          }
        ]
      }""")

      intercept[Exception]( new UnaryCheck(config, Some(new ActionFactory())) )
    }

    it("should succeed if onPass is missing") {
      val config = parse(s"""{
        "field": "A",
        "op": "not-empty",
        "onFail": [ 
          { 
            "actionType": "null"
          }
        ]
      }""")

      Try( new UnaryCheck(config, Some(new ActionFactory())) ).isSuccess should be (true)
    }

    it("should succeed if onFail is missing") {
      val config = parse(s"""{
        "field": "A",
        "op": "not-empty",
        "onPass": [ 
          { 
            "actionType": "null"
          }
        ]
      }""")

      Try( new UnaryCheck(config, Some(new ActionFactory())) ).isSuccess should be (true)
    }

    it("should fail if both onPass & onFail are missing") {
      val config = parse(s"""{
        "field": "A",
        "op": "not-empty"
      }""")
      intercept[Exception]( new UnaryCheck(config, Some(new ActionFactory())) )
    }

    it("should fail if there is no config section at all") {
      val config = parse("""{"A":"A"}""") \ "jnothing"
      intercept[Exception]( new UnaryCheck(config, Some(new ActionFactory())) )
    }

    it("should throw if right is specified for unary operator") {
      val config = parse(s"""{
        "left": "A",
        "op": "non-empty",
        "right": "B",
        "onFail": [ 
          { 
            "actionType": "null"
          }
        ]
      }""")

      intercept[Exception]( new UnaryCheck(config, Some(new ActionFactory())) )
    }
  }


  describe("getLookupRequirements") {

    it("should return default requirements") {

      // Null Action doesn't implement getLookupRequirements() so they should be
      // the same:
      val a = new UnaryCheck("fieldname", new MockChecker, makeNullAL)
      val nullAction = new NullAction()

      nullAction.getLookupRequirements should be (a.getLookupRequirements)
    }
  }

  describe("perform()") {

    val simpleConfig = parse(s"""{
        "field": "A",
        "op": "not-empty",
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
      val a = new UnaryCheck(simpleConfig, Some(new ActionFactory()))
      val onPassAction = getNullAction(a.onPass.actions.head)
      val onFailAction = getNullAction(a.onFail.actions.head)
      onPassAction.wasRun should be (false)
      onFailAction.wasRun should be (false)

      val result = a.perform(parse("""{"A": "X"}"""), Map.empty[String, String], ActionContext())

      onPassAction.wasRun should be (true)
      onFailAction.wasRun should be (false)
    }

    it("should only perform actions in onFail when test fails due to empty string") {
      val a = new UnaryCheck(simpleConfig, Some(new ActionFactory()))
      val onPassAction = getNullAction(a.onPass.actions.head)
      val onFailAction = getNullAction(a.onFail.actions.head)
      onPassAction.wasRun should be (false)
      onFailAction.wasRun should be (false)

      val result = a.perform(parse("""{"A": ""}"""), Map.empty[String, String], ActionContext())

      onPassAction.wasRun should be (false)
      onFailAction.wasRun should be (true)
    }

    it("should only perform actions in onFail when test fails due to nonexistent field in input") {
      val a = new UnaryCheck(simpleConfig, Some(new ActionFactory()))
      val onPassAction = getNullAction(a.onPass.actions.head)
      val onFailAction = getNullAction(a.onFail.actions.head)
      onPassAction.wasRun should be (false)
      onFailAction.wasRun should be (false)

      val result = a.perform(parse("""{"B": "BVal"}"""), Map.empty[String, String], ActionContext())

      onPassAction.wasRun should be (false)
      onFailAction.wasRun should be (true)
    }
  }

  describe("integration") {

    it("should create an UnaryCheck type with not-empty checker in the ActionFactory") {
      val factory = new ActionFactory()
      val items = factory.createItems("""{
        "activityType": "impressions",
        "items": [
          {
            "name": "test",
            "actions":[
              {
                "actionType":"check",
                "config": {
                  "field": "A",
                  "op": "empty",
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
                }
              }
            ]
          }
        ]
      }""")

      items.size should be (1)
      items.head.actions.size should be (1)
      val action = items.head.actions.head
      action match {
        case a: UnaryCheck => a.checker match {
          case c: EmptyChecker => ;
          case _ => fail()
        }
        case _ => fail()
      }
    }

    it("should run the action if specified in a configuration") {

      // using this fails - got NPE inside CachedLookupRequirement (!?)
      //val mockCheckNonEmpty = mock[CheckNonEmpty]
      //val actionFactory = new ActionFactory() {
      //  override def createAction(
      //    actionType: String,
      //    actionConfig: JValue):
      //    Option[Action] = {
      //
      //    actionType match {
      //      case "check-non-empty" => Some(mockCheckNonEmpty)
      //      case _ => None
      //    }
      //  }
      //}

      // instead fake-mock by adding an enriched field and checking it.

      val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
        List("""{ "activityMap": {"A": ""}}"""), // A is empty
        s"""{
          "activityType": "impressions",
          "items": [
            {
              "name": "test",
              "actions":[
                {
                  "actionType":"check",
                  "config": {
                    "field": "A",
                    "op": "empty",
                    "onPass": [
                      {
                        "actionType": "add-enriched-field",
                        "config": [
                          {
                            "key": "K",
                            "value": "PASS"
                          }
                        ]
                      }
                    ],
                    "onFail": [
                      {
                        "actionType": "add-enriched-field",
                        "config": [
                          {
                            "key": "K",
                            "value": "FAIL"
                          }
                        ]
                      }
                    ]
                  }
                }
              ]
            }
          ]
        }""",
        sc).collect()

      enriched.size should be (1) // always
      enriched.head should be (Map("K"->"PASS"))
    }
  }


}


