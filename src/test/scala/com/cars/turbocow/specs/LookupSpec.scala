package com.cars.turbocow

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.scalatest.junit.JUnitRunner
import com.cars.turbocow.actions._
import org.apache.spark.sql.hive._

import scala.io.Source

// Fix for Scalatest on Gradle:  (from http://stackoverflow.com/questions/18823855/cant-run-scalatest-with-gradle)
// Alternately, try using https://github.com/maiflai/gradle-scalatest
//@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class LookupSpec extends UnitSpec {

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

  describe("Lookup constructor")  // ------------------------------------------------
  {
    it("should parse a file correctly") {

      implicit val formats = org.json4s.DefaultFormats
      val configStr = """
        {
        	"activityType": "impressions",
        	"items": [
        		{
        			"source": [ "AField" ], 
        			"actions":[
        				{
        					"actionType":"lookup",
                            "config": {
                                "lookupFile": "./src/test/resources/testdimension-table-for-lookup.json",
                                "lookupField": "KEYFIELD",
                                "fieldsToSelect": [
                                  "EnhField1",
                                  "EnhField2",
                                  "EnhField3"
                                 ]
                            }
        				}
        			]
        		}
        	]
        }
      """

      val configAST = parse(configStr)
      val actionsList = ((configAST \ "items").children.head \ "actions")
      actionsList.children.size should be (1)

      val actionConfig = actionsList.children.head \ "config"
      actionConfig should not be (JNothing)

      // create the action and test all fields after construction:
      val action = Lookup(actionConfig, None, List.empty[String])
      action.lookupFile.get should be ("./src/test/resources/testdimension-table-for-lookup.json")
      action.lookupDB should be (None)
      action.lookupTable should be (None)
      action.lookupField should be ("KEYFIELD")
      action.fieldsToSelect should be (List("EnhField1", "EnhField2", "EnhField3"))
    }

  }

}




