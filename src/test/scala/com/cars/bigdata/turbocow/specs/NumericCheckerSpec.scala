package com.cars.bigdata.turbocow.specs

import com.cars.bigdata.turbocow.actions.checks.{CheckParams, NumericChecker}
import com.cars.bigdata.turbocow.test.SparkTestContext._
import com.cars.bigdata.turbocow.{ActionContext, ActionEngine, UnitSpec}
import org.json4s.jackson.JsonMethods._

class NumericCheckerSpec extends UnitSpec {

  val checker = new NumericChecker

  describe("NumericChecker") {

    def doCheck(inputJson: String): Boolean = {

      checker.performCheck(
        CheckParams("fieldA"),
        parse(inputJson),
        Map.empty[String, String],
        new ActionContext
      )
    }

    it("should return true if field is numeric") {
      doCheck("""{"fieldA": "597"}""") should be (true)
    }
    it("should return true if field is negative number") {
      doCheck("""{"fieldA": "-597"}""") should be (true)
    }
    it("should return true if field is float") {
      doCheck("""{"fieldA": "597.859"}""") should be (true)
    }
    it("should return true if field is negative decimal") {
      doCheck("""{"fieldA": "-0.597"}""") should be (true)
    }
    it("should return false if field is null") {
      doCheck("""{"fieldA": null}""") should be (false)
    }
    it("should return false if field is nonexistent") {
      doCheck("""{"X": ""}""") should be (false)
    }

    it("should return false if anything is in the field") {
      doCheck("""{"fieldA": "sjgfr3435"}""") should be (false)
    }

  }

  describe(" Unary Checks for numeric/non-numeric"){
    it("should run numeric action successfully") {

      val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
        List("""{ "activityMap": {"A": "175"}}"""), // A with an integer
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
                    "op": "numeric",
                    "onPass": [
                      {
                        "actionType": "add-enriched-field",
                        "config": [
                          {
                            "key": "XXX",
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
      enriched.head should be (Map("XXX"->"PASS"))
    }

    it("should run numeric action successfully with a float value") {

      val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
        List("""{ "activityMap": {"A": "175.43"}}"""), // A is float value
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
                    "op": "numeric",
                    "onPass": [
                      {
                        "actionType": "add-enriched-field",
                        "config": [
                          {
                            "key": "XXX",
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
      enriched.head should be (Map("XXX"->"PASS"))
    }

    it("should run numeric action successfully with negative float numbers") {

      val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
        List("""{ "activityMap": {"A": "-9999999999999999999.9999999999999999999"}}"""), // A is negative float value
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
                    "op": "numeric",
                    "onPass": [
                      {
                        "actionType": "add-enriched-field",
                        "config": [
                          {
                            "key": "XXX",
                            "value": "PASSED"
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
      enriched.head should be (Map("XXX"->"PASSED"))
    }

    it("should run numeric action with OnFail search and replace action ") {

      val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
        List("""{ "activityMap": {"A": "6754a9"}}"""), // A is non-numeric string.
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
                    "op": "numeric",
                    "onPass": [
                      {
                        "actionType": "add-enriched-field",
                        "config": [
                          {
                            "key": "XXX",
                            "value": "PASS"
                          }
                        ]
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
      enriched.head should be (Map("A"->"675499"))
    }

    //unnecessary test case for checking non-numeric action with inverse of numeric.
    // STMS have checking conditions as 'if non-numeric'. So this gives the configurator
    // flexibility of using numeric-onFail and non-numeirc-onPass (both act the same)
    it("should run non-numeric action with d in inputValue with OnPass search and replace action ") {

      val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
        List("""{ "activityMap": {"A": "547346d"}}"""), // A is non-numeric string.
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
                    "op": "non-numeric",
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
                            "searchFor": "d",
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
      enriched.head should be (Map("A"->"5473468"))
    }

    it("should run non-numeric D in inputValue action with OnPass search and replace action ") {

      val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
        List("""{ "activityMap": {"A": "547346D"}}"""), // A is non-numeric string.
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
                    "op": "non-numeric",
                    "onPass": [
                      {
                        "actionType": "search-and-replace",
                        "config":{
                            "inputSource" : [ "A" ],
                            "searchFor": "D",
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
                            "searchFor": "d",
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
      enriched.head should be (Map("A"->"5473468"))
    }

    it("should run non-numeric f in inputValue action with OnPass search and replace action ") {

      val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
        List("""{ "activityMap": {"A": "547346f"}}"""), // A is non-numeric string.
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
                    "op": "non-numeric",
                    "onPass": [
                      {
                        "actionType": "search-and-replace",
                        "config":{
                            "inputSource" : [ "A" ],
                            "searchFor": "f",
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
                            "searchFor": "d",
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
      enriched.head should be (Map("A"->"5473468"))
    }

    it("should run non-numeric F in inputValue action with OnPass search and replace action ") {

      val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
        List("""{ "activityMap": {"A": "547346F"}}"""), // A is non-numeric string.
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
                    "op": "non-numeric",
                    "onPass": [
                      {
                        "actionType": "search-and-replace",
                        "config":{
                            "inputSource" : [ "A" ],
                            "searchFor": "F",
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
                            "searchFor": "d",
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
      enriched.head should be (Map("A"->"5473468"))
    }

    it("should run non-numeric action with OnFail search and replace action. the input value has number ") {

      val enriched: Array[Map[String, String]] = ActionEngine.processJsonStrings(
        List("""{ "activityMap": {"A": "5672934"}}"""), // A is non-numeric string.
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
                    "op": "non-numeric",
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
      enriched.head should be (Map("A"->"5672934"))
    }
  }

}



