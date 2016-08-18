package com.cars.bigdata.turbocow.actions

import java.net.URI

import com.cars.bigdata.turbocow._
import com.cars.bigdata.turbocow.test.SparkTestContext._

/**
  * Created by rossstuart on 8/17/16.
  */
class AddScratchToEnrichedSpec
  extends UnitSpec
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


  describe("AddScratchedToEnriched"){
  it("should be able to enrich from the scratchpad when a test value is set") {
    val scratchPad: ScratchPad = new ScratchPad()
    scratchPad.set("timestamp","test123")
    scratchPad.set("appID", "applicationID_1234567890")
    val enriched: Array[Map[String, String]] = ActionEngine.processDir(
      new URI("./src/test/resources/input-integration.json"),
      """{
        |  "activityType":"impressions",
        |  "items":[
        |    {
        |      "actions":[
        |        {
        |          "actionType":"add-scratch-to-enriched",
        |           "config": {
        |            "keys": ["appID", "timestamp"]
        |             }
        |         }
        |      ]
        |    }
        |  ]
        |}""".stripMargin, sc,
      None,
      new ActionFactory(new CustomActionCreator),
      scratchPad).collect()

    enriched.size should be (1) // always one because there's only one json input object
    enriched.head("timestamp") should be ("test123")
    enriched.head("appID") should be ("applicationID_1234567890")
  }

  it("should return empty map in enriched when trying to enrich on an empty scratchpad") {
    val enriched: Array[Map[String, String]] = ActionEngine.processDir(
      new URI("./src/test/resources/input-integration.json"),
      """{
        |  "activityType":"impressions",
        |  "items":[
        |    {
        |      "actions":[
        |        {
        |          "actionType":"add-scratch-to-enriched"
        |        }
        |      ]
        |    }
        |  ]
        |}""".stripMargin, sc,
      None,
      new ActionFactory(new CustomActionCreator)).collect()

    enriched.size should be (0) // empty enriched records get filtered out
  }
  }
}
