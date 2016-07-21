package com.cars.bigdata.turbocow

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.scalatest.junit.JUnitRunner
import com.cars.bigdata.turbocow.actions._
import org.apache.spark.sql.hive._

import scala.io.Source
import test.SparkTestContext._

import scala.util.Try
import org.mockito.Mockito._

class CheckSpec extends UnitSpec {

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

    //it("should succeed if both onPass and onFail are specified") {
    //  val onPass = makeNullAL
    //  val onFail = makeNullAL
    //  val a = new Check(
    //    fieldName = "A", 
    //    onPass = onPass,
    //    onFail = onFail
    //  )
    //  a.fieldName should be ("A")
    //  a.onPass should be (onPass)
    //  a.onFail should be (onFail)
    //}
    //
    //it("should throw if fieldName is empty") {
    //  intercept[Exception]( new Check("") )
    //}
    //
    //it("should throw if fieldName is null") {
    //  intercept[Exception]( new Check(null) )
    //}
    //
    //it("should throw if neither onPass or onFail is specified") {
    //  intercept[Exception]( new Check("fieldName") )
    //}
    //
    //it("should not throw if onPass is there but onFail is missing") {
    //  Try( new Check("fieldName", onPass=makeNullAL) ).isSuccess should be (true)
    //}
    //
    //it("should not throw if onPass is missing but onFail is there") {
    //  Try( new Check("fieldName", onFail=makeNullAL) ).isSuccess should be (true)
    //}
    //
    //// todo add this test to ActionListSpec
    //it("should throw if onPass or onFail contains a null Action") {
    //  intercept[Exception]( new Check("fn", onPass=new ActionList(List(new NullAction, null))))
    //  intercept[Exception]( new Check("fn", onFail=new ActionList(List(new NullAction, null))))
    //  intercept[Exception]( new Check("fn", onPass=new ActionList(List(null, new NullAction))))
    //  intercept[Exception]( new Check("fn", onFail=new ActionList(List(null, new NullAction))))
    //}
  }

  describe("JSON constructor") {

    //it("should construct - happy path") {
    //  val config = parse(s"""{
    //    "left": "LEFT",
    //    "leftSource": "input"
    //    "op": "non-empty",
    //    "right": "RIGHT",
    //    "rightSource": "enriched",
    //    
    //    "onPass": [ 
    //      { 
    //        "actionType": "null",
    //        "config": { "name": "PASS" }
    //      }
    //    ],
    //    "onFail": [ 
    //      { 
    //        "actionType": "null",
    //        "config": { "name": "FAIL" }
    //      }
    //    ]
    //  }""")
    //
    //  val a = new Check(config, Some(new ActionFactory()))
    //
    //  a.left should be ("LEFT")
    //  a.leftSource should be (Some(FieldSource.Input))
    //
    //  a.operator match {
    //    case n: NonEmptyOperator => ;
    //    case _ => fail()
    //  }
    //
    //  a.right should be (Some("right"))
    //  a.rightSource should be (Some(FieldSource.Enriched))
    //
    //  a.onPass.actions.size should be (1)
    //  a.onFail.actions.size should be (1)
    //  val onPassAction = getNullAction(a.onPass.actions.head)
    //  val onFailAction = getNullAction(a.onFail.actions.head)
    //  onPassAction.name should be (Some("PASS"))
    //  onFailAction.name should be (Some("FAIL"))
    //
    //  // Note, this fails too:
    //  //List[Action](new NullAction(Some("A"))) should be (List[Action](new NullAction(Some("A"))))
    //}
    
    //it("should throw if fieldName is missing") {
    //  val config = parse(s"""{
    //    "onPass": [ 
    //      { 
    //        "actionType": "null",
    //        "config": { "name": "A PASS" }
    //      }
    //    ],
    //    "onFail": [ 
    //      { 
    //        "actionType": "null",
    //        "config": { "name": "A FAIL" }
    //      }
    //    ]
    //  }""")
    //
    //  intercept[Exception]( new Check(config, Some(new ActionFactory())) )
    //}
    //
    //it("should succeed if onPass is missing") {
    //  val config = parse(s"""{
    //    "fieldName": "A",
    //    "onFail": [ 
    //      { 
    //        "actionType": "null",
    //        "config": { "name": "A FAIL" }
    //      }
    //    ]
    //  }""")
    //
    //  Try( new Check(config, Some(new ActionFactory())) ).isSuccess should be (true)
    //}
    //
    //it("should succeed if onFail is missing") {
    //  val config = parse(s"""{
    //    "fieldName": "A",
    //    "onPass": [ 
    //      { 
    //        "actionType": "null",
    //        "config": { "name": "A PASS" }
    //      }
    //    ]
    //  }""")
    //
    //  Try( new Check(config, Some(new ActionFactory())) ).isSuccess should be (true)
    //}
    //
    //it("should fail if both onPass & onFail are missing") {
    //  val config = parse(s"""{
    //    "fieldName": "A"
    //  }""")
    //  intercept[Exception]( new Check(config, Some(new ActionFactory())) )
    //}
    //
    //it("should fail if there is no config section at all") {
    //  val config = parse("""{"A":"A"}""") \ "jnothing"
    //  intercept[Exception]( new Check(config, Some(new ActionFactory())) )
    //}

  }


  describe("getLookupRequirements") {

    //it("should return default requirements") {
    //
    //  // Null Action doesn't implement getLookupRequirements() so they should be
    //  // the same:
    //  val a = new Check("fieldname", makeNullAL)
    //  val nullAction = new NullAction()
    //
    //  nullAction.getLookupRequirements should be (a.getLookupRequirements)
    //}
  }

  describe("perform()") {

    //val simpleConfig = parse(s"""{
    //    "fieldName": "A",
    //    "onPass": [ 
    //      { 
    //        "actionType": "null",
    //        "config": { "name": "A PASS" }
    //      }
    //    ],
    //    "onFail": [ 
    //      { 
    //        "actionType": "null",
    //        "config": { "name": "A FAIL" }
    //      }
    //    ]
    //  }""")
    //
    //it("should only perform actions in onPass when test passes") {
    //  val a = new Check(simpleConfig, Some(new ActionFactory()))
    //  val onPassAction = getNullAction(a.onPass.actions.head)
    //  val onFailAction = getNullAction(a.onFail.actions.head)
    //  onPassAction.wasRun should be (false)
    //  onFailAction.wasRun should be (false)
    //
    //  val result = a.perform(parse("""{"A": "X"}"""), Map.empty[String, String], ActionContext())
    //
    //  onPassAction.wasRun should be (true)
    //  onFailAction.wasRun should be (false)
    //}
    //
    //it("should only perform actions in onFail when test fails due to empty string") {
    //  val a = new Check(simpleConfig, Some(new ActionFactory()))
    //  val onPassAction = getNullAction(a.onPass.actions.head)
    //  val onFailAction = getNullAction(a.onFail.actions.head)
    //  onPassAction.wasRun should be (false)
    //  onFailAction.wasRun should be (false)
    //
    //  val result = a.perform(parse("""{"A": ""}"""), Map.empty[String, String], ActionContext())
    //
    //  onPassAction.wasRun should be (false)
    //  onFailAction.wasRun should be (true)
    //}
    //
    //it("should only perform actions in onFail when test fails due to nonexistent field in input") {
    //  val a = new Check(simpleConfig, Some(new ActionFactory()))
    //  val onPassAction = getNullAction(a.onPass.actions.head)
    //  val onFailAction = getNullAction(a.onFail.actions.head)
    //  onPassAction.wasRun should be (false)
    //  onFailAction.wasRun should be (false)
    //
    //  val result = a.perform(parse("""{"B": "BVal"}"""), Map.empty[String, String], ActionContext())
    //
    //  onPassAction.wasRun should be (false)
    //  onFailAction.wasRun should be (true)
    //}
  }

  describe("integration") {

    //it("should create an Check type in the ActionFactory") {
    //  val factory = new ActionFactory()
    //  val items = factory.createItems("""{
    //    "activityType": "impressions",
    //    "items": [
    //      {
    //        "name": "test",
    //        "actions":[
    //          {
    //            "actionType":"check-non-empty",
    //            "config": {
    //              "fieldName": "A",
    //              "onPass": [
    //                {
    //                  "actionType": "null",
    //                  "config": { "name": "A PASS" }
    //                }
    //              ],
    //              "onFail": [
    //                {
    //                  "actionType": "null",
    //                  "config": { "name": "A FAIL" }
    //                }
    //              ]
    //            }
    //          }
    //        ]
    //      }
    //    ]
    //  }""")
    //
    //  items.size should be (1)
    //  items.head.actions.size should be (1)
    //  val action = items.head.actions.head
    //  action match {
    //    case a: Check => ;
    //    case _ => fail()
    //  }
    //}
    //
    //it("should run the action if specified in a configuration") {
    //
    //  // using this fails - got NPE inside CachedLookupRequirement (!?)
    //  //val mockCheck = mock[Check]
    //  //val actionFactory = new ActionFactory() {
    //  //  override def createAction(
    //  //    actionType: String,
    //  //    actionConfig: JValue):
    //  //    Option[Action] = {
    //  //
    //  //    actionType match {
    //  //      case "check-non-empty" => Some(mockCheck)
    //  //      case _ => None
    //  //    }
    //  //  }
    //  //}
    //
    //  // instead fake-mock by adding an enriched field and checking it.
    //
    //  val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
    //    List("""{ "activityMap": {"A": "notEmpty"}}"""),
    //    s"""{
    //      "activityType": "impressions",
    //      "items": [
    //        {
    //          "name": "test",
    //          "actions":[
    //            {
    //              "actionType":"check-non-empty",
    //              "config": {
    //                "fieldName": "A",
    //                "onPass": [
    //                  {
    //                    "actionType": "add-enriched-field",
    //                    "config": [
    //                      {
    //                        "key": "K",
    //                        "value": "PASS"
    //                      }
    //                    ]
    //                  }
    //                ],
    //                "onFail": [
    //                  {
    //                    "actionType": "add-enriched-field",
    //                    "config": [
    //                      {
    //                        "key": "K",
    //                        "value": "FAIL"
    //                      }
    //                    ]
    //                  }
    //                ]
    //              }
    //            }
    //          ]
    //        }
    //      ]
    //    }""",
    //    sc).collect()
    //
    //  enriched.size should be (1) // always
    //  enriched.head should be (Map("K"->"PASS"))
    //}

  }
}



