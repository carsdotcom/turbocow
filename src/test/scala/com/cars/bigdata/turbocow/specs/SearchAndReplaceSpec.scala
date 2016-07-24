package com.cars.bigdata.turbocow.specs

import com.cars.bigdata.turbocow.{test => _, _}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.scalatest.junit.JUnitRunner
import com.cars.bigdata.turbocow.actions._
import org.apache.spark.sql.hive._

import scala.io.Source
import com.cars.bigdata.turbocow.test.SparkTestContext._


/**
  * Created by nchaturvedula on 7/20/2016.
  */
class SearchAndReplaceSpec extends UnitSpec {


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

  describe("SearchAndReplace Action"){
    it("should successfully process searchFor-~-replaceWith-|") {

      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new java.net.URI("./src/test/resources/input.json"),
        """{
          |   "activityType":"impressions",
          |   "items":[
          |      {
          |         "actions":[
          |            {
          |               "actionType":"search-and-replace",
          |               "config":{
          |                 "inputSource" : ["adobe_id"],
          |                 "searchFor" : "~",
          |                 "replaceWith" : "|"
          |               }
          |            }
          |         ]
          |      }
          |   ]
          |}""".stripMargin,
        sc,
        None,
        new ActionFactory(new CustomActionCreator)).collect()


      enriched.head("adobe_id") should be ("[CS]v1|2B8185E9851937DD-40000605800EC3AC[CE]")
    }

    it("should successfully process searchFor any and replaceWith anything") {

      val enriched: Array[Map[String, String]] = ActionEngine.processDir(
        new java.net.URI("./src/test/resources/input.json"),
        """{
          |   "activityType":"impressions",
          |   "items":[
          |      {
          |         "actions":[
          |            {
          |               "actionType":"search-and-replace",
          |               "config":{
          |                 "inputSource" : ["adobe_id2"],
          |                 "searchFor" : "any",
          |                 "replaceWith" : "anything"
          |               }
          |            }
          |         ]
          |      }
          |   ]
          |}""".stripMargin,
        sc).collect()


      enriched.head("adobe_id2") should be ("~~~anything~~~")
    }
  }
}
