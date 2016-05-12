package com.cars.ingestionframework

import org.scalatest.junit.JUnitRunner

// Fix for Scalatest on Gradle:  (from http://stackoverflow.com/questions/18823855/cant-run-scalatest-with-gradle)
// Alternately, try using https://github.com/maiflai/gradle-scalatest
//@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ActionFactorySpec extends UnitSpec {

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

  val actionFactory = new ActionFactoryForTest()

  describe("ActionListFactory.create")  // ------------------------------------------------
  {
    it("should successfully parse a 1-source, 1-action config file") {

      val configFile = resourcesDir + "testconfig-1source-1action.json"
      val itemList: List[SourceAction] = actionFactory.create(configFile)
      itemList.size should be (1)
      itemList.head.source should be (List("AField"))
      itemList.head.actions.size should be (1)

      itemList.head.actions.head match {
        case a: Action1 => ;
        case _ => fail()
      }
    }

    it("should successfully parse a 2-source, 1-action config file") {
      val configFile = resourcesDir + "testconfig-2source-1action.json"
      val itemList: List[SourceAction] = actionFactory.create(configFile)
      itemList.size should be (1)
      itemList.head.source should be (List("AField", "BField"))
      itemList.head.actions.size should be (1)

      itemList.head.actions.head match {
        case a: Action1 => ;
        case _ => fail()
      }
    }

    //it("should retrieve what was stored") {
    //  dataStore.getStore.size should be(0)
    //  dataStore.store("key", "value")
    //  dataStore.getStore.size should be(1)
    //  dataStore.retrieve("key") should be (Some("value"))
    //}
  }

  //describe("DataStore.set")  // ------------------------------------------------
  //{
  //  it("should store what is given") {
  //    dataStore.getStore.size should be(0)
  //    dataStore.store("key", "value")
  //    dataStore.getStore.size should be(1)
  //    dataStore.getStore.get("key") should be(Some("value"))
  //  }
  //}
}



