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

class AvroOutputWriterSsspec 
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
  describe("write") {

    it("should only output the fields in the schema regardless of what is in the input RDD") {
      val enriched: RDD[Map[String, String]] = ActionEngine.processDir(
        new URI("./src/test/resources/input-integration.json"),
        """{
            "activityType": "impressions",
            "items": [
              {
                "actions":[{
                    "actionType":"simple-copy",
                    "config": {
                      "inputSource": [ "AField", "BField" ]
                    }
                  }
                ]
              }
            ]
          }
        """,
        sc).persist()
  
      // this should be the enriched record:

      val enrichedAll = enriched.collect()
      enrichedAll.size should be (1) // always one because there's only one json input object
      enrichedAll.head.size should be (2)
      enrichedAll.head.get("AField") should be (Some("A"))
      enrichedAll.head.get("BField") should be (Some("B"))

      // now write to avro
      //val tempFile = File.createTempFile("testoutput-", ".avro", null).deleteOnExit()
      val outputDir = { 
        val dir = Files.createTempDirectory("testoutput-")
        new File(dir.toString).delete()
        dir.toString
      }
      println("%%%%%%%%%%%%%%%%%%%%%%%%% outputDir = "+outputDir.toString)

      // write
      AvroOutputWriter.write(enriched, List("BField"), outputDir.toString, sc)

      // now read what we wrote
      val rows: Array[Row] = sqlCtx.read.avro(outputDir.toString).collect()
      rows.size should be (1) // one row only
      val row = rows.head
      row.size should be (1) // only one field in that row
      Try( row.getAs[String]("AField") ).isFailure should be (true)
      Try( row.getAs[String]("BField") ) should be (Success("B"))
    }
  }

}

