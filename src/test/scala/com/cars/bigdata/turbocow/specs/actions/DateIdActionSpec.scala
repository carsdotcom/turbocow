package com.cars.bigdata.turbocow.specs.actions

import com.cars.bigdata.turbocow._
import com.cars.bigdata.turbocow.test.SparkTestContext._

import scala.io.Source

class DateIdActionSpec
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
    super.afterEach()
  }

  // after all tests have run
  override def afterAll() = {
    super.afterAll()
  }

  /** Helper fn
    */
  def fileToString(filePath: String) = Source.fromFile(filePath).getLines.mkString

  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  // Tests start
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////



  describe("dateIdAction ") {

    it("should process dateIdAction and enhance the dateId field when dateId set on intial scratchpad") {

      val scratchPad: ScratchPad = new ScratchPad()
      scratchPad.set("dateId","20160101")
      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration.json",
        """{
          |  "activityType":"impressions",
          |  "items":[
          |    {
          |      "actions":[
          |        {
          |          "actionType":"date-id-action",
          |          "config": {
          |              "inputSource": ["year", "month", "day"],
          |              "outputTarget": "date_id"
          |            }
          |        }
          |      ]
          |    }
          |  ]
          |}""".stripMargin, sc,
        None,
        new ActionFactory(new CustomActionCreator),
        scratchPad).collect()

      enriched.size should be (1) // always one because there's only one json input object
      enriched.head("dateId") should be ("20160101")

    }

    it("should process dateIdAction and return empty map in enriched when dateId not set in intitial scratchpad") {

      val enriched: Array[Map[String, String]] = ActionEngine.process(
        "./src/test/resources/input-integration.json",
        """{
          |  "activityType":"impressions",
          |  "items":[
          |    {
          |      "actions":[
          |        {
          |          "actionType":"date-id-action",
          |          "config": {
          |              "inputSource": ["year", "month", "day"],
          |              "outputTarget": "date_id"
          |            }
          |        }
          |      ]
          |    }
          |  ]
          |}""".stripMargin, sc,
        None,
        new ActionFactory(new CustomActionCreator)).collect()

      enriched.size should be (1) // always one because there's only one json input object
      enriched.head.size should be (0)
    }

  }


}

 
