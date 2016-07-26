package com.cars.bigdata.turbocow.specs

import com.cars.bigdata.turbocow.actions._
import com.cars.bigdata.turbocow.actions.checks.{NumericChecker, TrueChecker}
import com.cars.bigdata.turbocow.test.SparkTestContext._
import com.cars.bigdata.turbocow.{ActionContext, ActionEngine, UnitSpec}
import org.json4s.jackson.JsonMethods._

class TrueCheckerSpec extends UnitSpec {

  val checker = new TrueChecker

  describe("TrueChecker") {

    def doCheck(inputJson: String): Boolean = {

      checker.performCheck(
        CheckParams("fieldA"),
        parse(inputJson),
        Map.empty[String, String],
        new ActionContext
      )
    }

    it("should return true if field is true") {
      doCheck("""{"fieldA": "true"}""") should be (true)
    }
    it("should return false if field is null") {
      doCheck("""{"fieldA": null}""") should be (false)
    }
    it("should return false if field is nonexistent") {
      doCheck("""{"X": ""}""") should be (false)
    }

    it("should return false if anything is in the field except true") {
      doCheck("""{"fieldA": "sjgfr3435"}""") should be (false)
    }
    it("should return false if false is in the field") {
      doCheck("""{"fieldA": "false"}""") should be (false)
    }
  }

  describe("True Checker Action from Config"){
    // true should be passed as Yes.
    it("should run true checker action successfully onPass ") {

      val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
        List("""{ "activityMap": {"A": "true"}}"""), // A is negative float value
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
                    "op": "true",
                    "onPass": [
                      {
                        "actionType": "add-enriched-field",
                        "config": [
                          {
                            "key": "XXX",
                            "value": "Yes"
                          }
                        ]
                      }
                    ],
                    "onFail": [
                      {
                        "actionType": "add-enriched-field",
                        "config": [
                          {
                            "key": "XXX",
                            "value": "No"
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
      enriched.head should be (Map("XXX"->"Yes"))
    }

    //anyvalue other than true should be passed as No.
    it("should run True Checker action with OnFail: anything other than true should be No ") {

      val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
        List("""{ "activityMap": {"A": "something something"}}"""), // A is non-numeric string.
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
                    "op": "true",
                    "onPass": [
                      {
                        "actionType": "add-enriched-field",
                        "config": [
                          {
                            "key": "XXX",
                            "value": "Yes"
                          }
                        ]
                      }
                    ],
                    "onFail": [
                      {
                        "actionType": "add-enriched-field",
                        "config": [{
                          "key": "XXX",
                          "value": "No"
                        }]
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
      enriched.head should be (Map("XXX"->"No"))
    }

    it("should run true checker action successfully onPass and pass it as true") {

      val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
        List("""{ "activityMap": {"A": "true"}}"""), // A is negative float value
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
                    "op": "true",
                    "onPass": [
                      {
                        "actionType": "add-enriched-field",
                        "config": [
                          {
                            "key": "XXX",
                            "value": "true"
                          }
                        ]
                      }
                    ],
                    "onFail": [
                      {
                        "actionType": "add-enriched-field",
                        "config": [
                          {
                            "key": "XXX",
                            "value": "false"
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
      enriched.head should be (Map("XXX"->"true"))
    }

    //anyvalue other than true should be passed as false.
    it("should run true checker action successfully onFail should output false ") {

      val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
        List("""{ "activityMap": {"A": "some other value"}}"""), // A is negative float value
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
                    "op": "true",
                    "onPass": [
                      {
                        "actionType": "add-enriched-field",
                        "config": [
                          {
                            "key": "XXX",
                            "value": "true"
                          }
                        ]
                      }
                    ],
                    "onFail": [
                      {
                        "actionType": "add-enriched-field",
                        "config": [
                          {
                            "key": "XXX",
                            "value": "false"
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
      enriched.head should be (Map("XXX"->"false"))
    }

    //unnecessary test case for checking non-numeric action with inverse of numeric.
    // STMS have checking conditions as 'check false'. So this gives the configurator
    // flexibility of using numeric-onFail and non-numeirc-onPass (both act the same)
    it("should run false action with OnPass search and replace action ") {

      val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
        List("""{ "activityMap": {"A": "false"}}"""), // A is non-numeric string.
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
                    "op": "false",
                    "onPass": [
                      {
                        "actionType": "search-and-replace",
                        "config":{
                            "inputSource" : [ "A" ],
                            "searchFor": "fal",
                            "replaceWith": "XYZ"
                          }
                      }
                    ],
                    "onFail": [
                      {
                        "actionType": "search-and-replace",
                        "config": [
                          {
                            "inputSource" : [ "A" ],
                            "searchFor": "a",
                            "replaceWith": "9"
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
      enriched.head should be (Map("A"->"XYZse"))
    }

    it("should run flase action with OnFail search and replace action. the input value has number ") {

      val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
        List("""{ "activityMap": {"A": "true"}}"""), // A is non-numeric string.
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
                    "op": "false",
                    "onPass": [
                      {
                        "actionType": "search-and-replace",
                        "config":{
                            "inputSource" : [ "A" ],
                            "searchFor": "d",
                            "replaceWith": "8"
                          }
                      }
                    ],
                    "onFail": [
                      {
                        "actionType": "search-and-replace",
                        "config": [
                          {
                            "inputSource" : [ "A" ],
                            "searchFor": "e",
                            "replaceWith": "9"
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
      enriched.head should be (Map("A"->"tru9"))
    }
  }
}



