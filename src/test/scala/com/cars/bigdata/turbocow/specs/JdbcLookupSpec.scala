package com.cars.bigdata.turbocow.actions

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.scalatest.junit.JUnitRunner
import com.cars.bigdata.turbocow._
import com.cars.bigdata.turbocow.test.SparkTestContext
import org.apache.spark.sql.hive._

import scala.util.Try

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

  import JdbcLookup._

  describe("constructor") {
    it("should not throw if the where clause includes expected $location tags") {
      Try{ new JdbcLookup(
        parse("""{
          "jdbcClient": "Hive",
          "select": [ "fieldA", "fieldB" ],
          "fromDBTable": "Db.Table",
          "where": "someA = '$input.someFieldA' and someB = '$enriched.someFieldB' and someC='$scratchpad.someFieldC' and someD='someFieldD'",
          "onPass": [{ "actionType": "null-action" }]
        }"""),
        Option(new ActionFactory(new CustomActionCreator))
      )}.isSuccess should be (true)
    }
    it("should throw if the where clause includes unexpected $location tags") {
      intercept[Exception] {new JdbcLookup(
        parse("""{
          "jdbcClient": "Hive",
          "select": [ "fieldA", "fieldB" ],
          "fromDBTable": "Db.Table",
          "where": "someA = '$input.someFieldA' and someB = '$enrichedX.someFieldB' and someC='$scratchpad.someFieldC'",
          "onPass": [{ "actionType": "null-action" }]
        }"""),
        Option(new ActionFactory(new CustomActionCreator))
      )}
    }
  }

  describe("checkWhere") {
    it("should fail on bad input") {

      val badWhere = List(
        """someA = '$$input.someFieldA' and someB = '$enriched.someFieldB'""",
        """someA = 'input.someFieldA' and someB = '$enriched.someFieldB'""",
        """someA = '$inputsomeFieldA' and someB = '$enriched.someFieldB'""",
        """someA = '$input.someFieldA' and someB = 'enriched.someFieldB'""",
        """someA = '$input.someFieldA' and someB = ''$enriched.someFieldB'"""
        // no way to check this without parsing EVERYTHING, which we want to avoid
        //"""someA = '$input.someFieldA' and someB = "$enriched.someFieldB""""
      )
      badWhere.foreach{ str=> 
        println("str = "+str)
        Try{ checkWhere(str) }.isSuccess should be (false)
      }
    }
  }

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

  describe("getLookupRequirements") {

    it("should aggregate properly") {
      val a = new JdbcLookup(
        parse(s"""{
          "jdbcClient": "Hive",
          "select": [ "fieldA", "fieldB" ],
          "fromDBTable": "Db.Table",
          "where": "something = 'somethingElse'",
          "onPass": [{ 
            "actionType": "lookup",
            "config": {
              "select": [
                "A1",
                "A2"
              ],
              "fromDBTable": "db.testTable",
              "where": "AKEY",
              "equals": "AAA"
            } 
          }],
          "onFail": [{ 
            "actionType": "lookup",
            "config": {
              "select": [
                "B1",
                "B2"
              ],
              "fromDBTable": "db.testTable",
              "where": "BKEY",
              "equals": "BBB"
            } 
          }]
        }"""),
        Option(new ActionFactory(new CustomActionCreator))
      )
      a.onPass.actions.size should be (1)
      a.onFail.actions.size should be (1)
      a.getLookupRequirements should be (List(
        CachedLookupRequirement(
          "db.testTable", 
          List("AKEY"), 
          List("A1", "A2")
        ),
        CachedLookupRequirement(
          "db.testTable", 
          List("BKEY"), 
          List("B1", "B2")
        )
      ))
    }
  }

  describe("function that converts string into value") {

    it("should default to $constant if not specified") {
      fail()
    }
  }

}

