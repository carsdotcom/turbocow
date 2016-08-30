package com.cars.bigdata.turbocow

import java.io.{BufferedWriter, File, FileWriter}
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

class AvroOutputWriterSpec
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

  import FileUtil._

  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  // Tests start
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  describe("write") {

    it("should only output the fields in the schema regardless of what is in the input RDD") {

      val avroSchema = """{
          "namespace": "ALS",
          "type": "record",
          "name": "impression",
          "fields": [
          {
            "name": "CField",
            "type": [ "string" ],
            "doc": "Type of the consumer activity. It is always IMPRESSION",
            "default": ""
          },
          {
            "name": "DField",
            "type": [ "null", "int" ],
            "doc": "Date when impression activity happened. Format of the date is yyyy-mm-dd",
            "default": 0
          }
        ],
        "doc": ""
      }"""
      val avroFile = writeTempFile(avroSchema, "avroschema.avsc")

      val enriched: RDD[Map[String, String]] = ActionEngine.processJsonStrings(
        List("""{ "md": { "AField": "A", "BField": "B" }, "activityMap": { "CField": "C", "DField": 11, "EField": true }}"""),
        """{
            "activityType": "impressions",
            "items": [
              {
                "actions":[{
                    "actionType":"simple-copy",
                    "config": {
                      "inputSource": [ "BField", "CField" ]
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
      //println("========= enrichedAll = "+enrichedAll.mkString("//"))
      enrichedAll.size should be (1) // always one because there's only one json input object
      enrichedAll.head.size should be (2)
      enrichedAll.head.get("BField") should be (Some("B"))
      enrichedAll.head.get("CField") should be (Some("C"))

      // now write to avro
      //val tempFile = File.createTempFile("testoutput-", ".avro", null).deleteOnExit()
      val outputDir = { 
        val dir = Files.createTempDirectory("testoutput-")
        new File(dir.toString).delete()
        dir.toString
      }
      println("%%%%%%%%%%%%%%%%%%%%%%%%% outputDir = "+outputDir.toString)

      // write
      AvroOutputWriter.write(enriched, avroFile, outputDir.toString, sc)

      // now read what we wrote
      val rows: Array[Row] = sqlCtx.read.avro(outputDir.toString).collect()
      //println("======== rows = ")
      rows.size should be (1) // one row only
      val row = rows.head
      row.size should be (2) // only one field in that row
      Try( row.getAs[String]("AField") ).isFailure should be (true)
      Try( row.getAs[String]("BField") ).isFailure should be (true)
      Try( row.getAs[String]("CField") ) should be (Success("C"))
    }
  }

}

