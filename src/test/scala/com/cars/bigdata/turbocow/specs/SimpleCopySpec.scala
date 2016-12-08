package com.cars.bigdata.turbocow

import java.io.File
import java.net.URI
import java.nio.file.Files

import com.cars.bigdata.turbocow.actions._
import com.cars.bigdata.turbocow.test.SparkTestContext._
import com.databricks.spark.avro._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.io.Source
import scala.util.{Success, Try}

import org.json4s._
import org.json4s.jackson.JsonMethods._

class SimpleCopySpec 
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

  describe("perform()") // ------------------------------------------------
  {
    it("should successfully process one field") {
      val action: Action = new SimpleCopy(List("AField"))
      val enriched = action.perform(
        // input:
        parse("""{ "AField": "A", "BField": "B", "CField": 10, "DField": 11, "EField": ""}"""),
        // current enriched map:
        Map.empty[String, String],
        ActionContext()
      ).enrichedUpdates

      enriched.size should be (1)
      enriched.get("AField") should be (Some("A"))
    }

    it("should successfully process two fields") {
    
      val action: Action = new SimpleCopy(List("AField", "CField"))
      val enriched = action.perform(
        // input:
        parse("""{ "AField": "A", "BField": "B", "CField": 10, "DField": 11, "EField": ""}"""),
        // current enriched map:
        Map.empty[String, String],
        ActionContext()
      ).enrichedUpdates

      enriched.size should be (2)
      enriched.get("AField") should be (Some("A"))
      enriched.get("CField") should be (Some("10"))
    }

    it("should successfully copy over a field even if it is blank in the input") {
      val action: Action = new SimpleCopy(List("AField"))
      val enriched = action.perform(
        // input:
        parse("""{ "AField": "", "BField": "B", "CField": 10, "DField": 11, "EField": ""}"""),
        // current enriched map:
        Map.empty[String, String],
        ActionContext()
      ).enrichedUpdates

      enriched.size should be (1)
      enriched.get("AField") should be (Some(""))
    }

    it("""should successfully copy over a field even if it is " " in the input""") {
    
      val action: Action = new SimpleCopy(List("AField"))
      val enriched = action.perform(
        // input:
        parse("""{ "AField": " ", "BField": "B", "CField": 10, "DField": 11, "EField": ""}"""),
        // current enriched map:
        Map.empty[String, String],
        ActionContext()
      ).enrichedUpdates

      enriched.size should be (1)
      enriched.get("AField") should be (Some(" "))
    }

    it("""should copy over a field even if it is null in the input""") {

      // Note, copying a null value is a slight, momentary waste of memory, 
      // because when the record is done processing, all input fields in the schema
      // are copied in regardless.
    
      val action: Action = new SimpleCopy(List("AField", "BField"))
      val enriched = action.perform(
        // input:
        parse("""{ "AField": null, "BField": "B", "CField": 10, "DField": 11, "EField": ""}"""),
        // current enriched map:
        Map.empty[String, String],
        ActionContext()
      ).enrichedUpdates

      enriched.size should be (2)
      enriched.get("AField") should be (Some(null))
      enriched.get("BField") should be (Some("B"))
    }

    it("should fail parsing missing config") {

      // todo call constructor rather than ActionEngine for these tests

      val e = intercept[Exception] {
        ActionEngine.processJsonStrings(
          Seq(s"""{ "md": { "AField": "A", "BField": "B" }, "activityMap": { "CField": 10, "DField": 11, "EField": "" }}"""),
          """{
              "activityType": "impressions",
              "items": [
                {
                  "actions":[{
                      "actionType":"simple-copy" }]}]}""",
          sc)
      }
    }
    it("should fail parsing empty list") {
      val e = intercept[Exception] {
        ActionEngine.processJsonStrings(
          Seq(s"""{ "md": { "AField": "A", "BField": "B" }, "activityMap": { "CField": 10, "DField": 11, "EField": "" }}"""),
          """{
              "activityType": "impressions",
              "items": [
                {
                  "actions":[{
                      "actionType":"simple-copy",
                      "config": {
                        "inputSource": [ ] }}]}]}""",
          sc)
      }
    }
    it("should fail parsing list with empty element") {
      val e = intercept[Exception] {
        ActionEngine.processJsonStrings(
          Seq(s"""{ "md": { "AField": "A", "BField": "B" }, "activityMap": { "CField": 10, "DField": 11, "EField": "" }}"""),
          """{
              "activityType": "impressions",
              "items": [
                {
                  "actions":[{
                      "actionType":"simple-copy",
                      "config": {
                        "inputSource": [ "A", "" ] }}]}]}""",
          sc)
      }
    }
    it("should fail parsing list with null element") {
      val e = intercept[Exception] {
        ActionEngine.processJsonStrings(
          Seq(s"""{ "md": { "AField": "A", "BField": "B" }, "activityMap": { "CField": 10, "DField": 11, "EField": "" }}"""),
          """{
              "activityType": "impressions",
              "items": [
                {
                  "actions":[{
                      "actionType":"simple-copy",
                      "config": {
                        "inputSource": [ "A", null ] }}]}]}""",
          sc)
      }
    }
  }
}

 

