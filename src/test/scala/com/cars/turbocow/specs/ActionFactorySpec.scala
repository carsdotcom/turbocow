package com.cars.turbocow

import scala.io.Source

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
  describe("ActionListFactory.createSourceActions")  // ------------------------------------------------
  {
    it("should successfully parse a 1-source, 1-action config file") {

      val actionFactory = new ActionFactoryForTest
      val config = Source.fromFile(resourcesDir + "testconfig-1source-1action.json").getLines().mkString("")
      val itemList: List[SourceAction] = actionFactory.createSourceActions(config)
      itemList.size should be (1)
      itemList.head.source should be (List("AField"))
      itemList.head.actions.size should be (1)

      itemList.head.actions.head match {
        case a: Action1 => ;
        case _ => fail()
      }
    }

    it("should successfully parse a 2-source, 1-action config file") {
      val actionFactory = new ActionFactoryForTest
      val config = Source.fromFile(resourcesDir + "testconfig-2source-1action.json").getLines().mkString("")
      val itemList: List[SourceAction] = actionFactory.createSourceActions(config)
      itemList.size should be (1)
      itemList.head.source should be (List("AField", "BField"))
      itemList.head.actions.size should be (1)

      itemList.head.actions.head match {
        case a: Action1 => ;
        case _ => fail()
      }
    }

    it("should successfully parse a 2-source, 2-action config file") {
      val actionFactory = new ActionFactoryForTest
      val config = Source.fromFile(resourcesDir + "testconfig-2source-2action.json").getLines().mkString("")
      val itemList: List[SourceAction] = actionFactory.createSourceActions(config)
      itemList.size should be (1)
      itemList.head.source should be (List("AField", "BField"))
      itemList.head.actions.size should be (2)

      itemList.head.actions.head match {
        case a: Action1 => ;
        case _ => fail()
      }
      itemList.head.actions.last match {
        case a: Action2 => ;
        case _ => fail()
      }
    }

    it("should successfully parse a 2-item config file") {
      val actionFactory = new ActionFactoryForTest
      val config = Source.fromFile(resourcesDir + "testconfig-2item.json").getLines().mkString("")
      val itemList: List[SourceAction] = actionFactory.createSourceActions(config)
      itemList.size should be (2)

      // check first item (head)
      itemList(0).source should be (List("AField", "BField"))
      itemList(0).actions.size should be (1)
      itemList(0).actions.head match {
        case a: Action1 => ;
        case _ => fail()
      }

      // check 2nd item
      itemList(1).source should be (List("CField", "DField"))
      itemList(1).actions.size should be (2)
      itemList(1).actions(0) match {
        case a: Action1 => ;
        case _ => fail()
      }
      itemList(1).actions(1) match {
        case a: Action2 => ;
        case _ => fail()
      }
    }

    it("should successfully parse a file with custom and standard actions") {
      //val actionFactory = new ActionFactory(List(new CustomActionCreator))
      val actionFactory = new ActionFactoryForTest(List(new CustomActionCreator))
      val config = Source.fromFile(resourcesDir + "testconfig-custom-and-standard.json").getLines().mkString("")
      val itemList: List[SourceAction] = actionFactory.createSourceActions(config)
      itemList.size should be (1)

      // check first item (head)
      itemList(0).source should be (List("AField", "BField"))
      itemList(0).actions.size should be (2)
      itemList(0).actions(0) match {
        case a: Custom1 => ;
        case _ => fail()
      }
      itemList(0).actions(1) match {
        case a: Action1 => ;
        case _ => fail()
      }
    }
  }

}



