package com.cars.bigdata.turbocow.actions

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.scalatest.junit.JUnitRunner
import com.cars.bigdata.turbocow._
import com.cars.bigdata.turbocow.test.SparkTestContext
import org.apache.spark.sql.hive._

class JdbcLookupSpec extends UnitSpec {

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

  describe("json constructor")  // ------------------------------------------------
  {
    it("should parse a json config (happy path)") {
      val a = new JdbcLookup(
        parse(s"""{
          "jdbcClient": "Hive",
          "select": [ "fieldA", "fieldB" ],
          "fromDBTable": "Db.Table",
          "where": "something = 'somethingElse'",
          "onPass": [{ "actionType": "null-action" }]
        }"""),
        Option(new ActionFactory(new CustomActionCreator))
      )
      a.jdbcClient should be ("Hive")
      a.select should be (List("fieldA", "fieldB"))
      a.fromDBTable should be ("Db.Table")
      a.where should be ("something = 'somethingElse'")
      a.onPass.actions.size should be (1)
    }

    it("should throw if jdbcClient is missing, empty, or null") {
      intercept[Exception]( new JdbcLookup(
        parse(s"""{
          "select": [ "fieldA", "fieldB" ],
          "fromDBTable": "Db.Table",
          "where": "something = 'somethingElse'",
          "onPass": [{ "actionType": "null-action" }]
        }"""),
        Option(new ActionFactory(new CustomActionCreator))
      ))
      intercept[Exception]( new JdbcLookup(
        parse(s"""{
          "jdbcClient": "",
          "select": [ "fieldA", "fieldB" ],
          "fromDBTable": "Db.Table",
          "where": "something = 'somethingElse'",
          "onPass": [{ "actionType": "null-action" }]
        }"""),
        Option(new ActionFactory(new CustomActionCreator))
      ))
      intercept[Exception]( new JdbcLookup(
        parse(s"""{
          "jdbcClient": null,
          "select": [ "fieldA", "fieldB" ],
          "fromDBTable": "Db.Table",
          "where": "something = 'somethingElse'",
          "onPass": [{ "actionType": "null-action" }]
        }"""),
        Option(new ActionFactory(new CustomActionCreator))
      ))
    }

    it("should throw if select is missing, empty, or null") {
      intercept[Exception]( new JdbcLookup(
        parse(s"""{
          "jdbcClient": "Hive",
          "fromDBTable": "Db.Table",
          "where": "something = 'somethingElse'",
          "onPass": [{ "actionType": "null-action" }]
        }"""),
        Option(new ActionFactory(new CustomActionCreator))
      ))
      intercept[Exception]( new JdbcLookup(
        parse(s"""{
          "jdbcClient": "Hive",
          "select": [ ],
          "fromDBTable": "Db.Table",
          "where": "something = 'somethingElse'",
          "onPass": [{ "actionType": "null-action" }]
        }"""),
        Option(new ActionFactory(new CustomActionCreator))
      ))
      intercept[Exception]( new JdbcLookup(
        parse(s"""{
          "jdbcClient": "Hive",
          "select": null,
          "fromDBTable": "Db.Table",
          "where": "something = 'somethingElse'",
          "onPass": [{ "actionType": "null-action" }]
        }"""),
        Option(new ActionFactory(new CustomActionCreator))
      ))
    }

    it("should throw if fromDBTable is missing, empty, or null") {
      intercept[Exception]( new JdbcLookup(
        parse(s"""{
          "jdbcClient": "Hive",
          "select": [ "fieldA", "fieldB" ],
          "where": "something = 'somethingElse'",
          "onPass": [{ "actionType": "null-action" }]
        }"""),
        Option(new ActionFactory(new CustomActionCreator))
      ))
      intercept[Exception]( new JdbcLookup(
        parse(s"""{
          "jdbcClient": "Hive",
          "select": [ "fieldA", "fieldB" ],
          "fromDBTable": "",
          "where": "something = 'somethingElse'",
          "onPass": [{ "actionType": "null-action" }]
        }"""),
        Option(new ActionFactory(new CustomActionCreator))
      ))
      intercept[Exception]( new JdbcLookup(
        parse(s"""{
          "jdbcClient": "Hive",
          "select": [ "fieldA", "fieldB" ],
          "fromDBTable": null,
          "where": "something = 'somethingElse'",
          "onPass": [{ "actionType": "null-action" }]
        }"""),
        Option(new ActionFactory(new CustomActionCreator))
      ))
    }

    it("should throw if where is missing, empty, or null") {
      intercept[Exception]( new JdbcLookup(
        parse(s"""{
          "jdbcClient": "Hive",
          "select": [ "fieldA", "fieldB" ],
          "fromDBTable": "Db.Table",
          "onPass": [{ "actionType": "null-action" }]
        }"""),
        Option(new ActionFactory(new CustomActionCreator))
      ))
      intercept[Exception]( new JdbcLookup(
        parse(s"""{
          "jdbcClient": "Hive",
          "select": [ "fieldA", "fieldB" ],
          "fromDBTable": "Db.Table",
          "where": "",
          "onPass": [{ "actionType": "null-action" }]
        }"""),
        Option(new ActionFactory(new CustomActionCreator))
      ))
      intercept[Exception]( new JdbcLookup(
        parse(s"""{
          "jdbcClient": "Hive",
          "select": [ "fieldA", "fieldB" ],
          "fromDBTable": "Db.Table",
          "where": null,
          "onPass": [{ "actionType": "null-action" }]
        }"""),
        Option(new ActionFactory(new CustomActionCreator))
      ))
    }

    it("should throw if no onPass or onFail is specified") {
      intercept[Exception]( new JdbcLookup(
        parse(s"""{
          "jdbcClient": "Hive",
          "select": [ "fieldA", "fieldB" ],
          "fromDBTable": "Db.Table",
          "where": "something = 'somethingElse'"
        }"""),
        Option(new ActionFactory(new CustomActionCreator))
      ))
    }
  }

//  describe("") {
//
//    it("") {
//    }
//  }
}





