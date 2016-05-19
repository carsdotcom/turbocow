package com.cars.ingestionframework

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.scalatest.junit.JUnitRunner
import com.cars.ingestionframework.actions._
import com.cars.ingestionframework.exampleapp.ExampleAppSpec
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
      val configFile = resourcesDir + "testconfig-integration-lookup.json"
      val configAST = parse(Source.fromFile(configFile).getLines.mkString)
      val actionsList = ((configAST \ "items").children.head \ "actions")
      actionsList.children.size should be (1)

      val actionConfig = actionsList.children.head \ "config"
      actionConfig should not be (JNothing)

      // create the action and test all fields after construction:
      val action = new Lookup(actionConfig, None)
      action.lookupFile.extract[String] should be ("./src/test/resources/testdimension-table-for-lookup.json")
      action.lookupDB should be (JNothing)
      action.lookupTable should be (JNothing)
      action.lookupField should be ("KEYFIELD")
      action.fieldsToSelect should be (List("EnhField1", "EnhField2", "EnhField3"))
    }

  }

}




