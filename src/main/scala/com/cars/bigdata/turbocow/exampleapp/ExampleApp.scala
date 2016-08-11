package com.cars.bigdata.turbocow.exampleapp

import java.io.Serializable
import java.lang.{Boolean, Double, Long}
import java.text.SimpleDateFormat
import java.util
import java.util.Map.Entry
import java.util.Calendar
import java.text.SimpleDateFormat
import com.cars.bigdata.turbocow._
import com.cars.bigdata.turbocow.actions._

import java.net.URI
import scala.collection.immutable.HashMap
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.json4s._
import org.json4s.jackson.JsonMethods._
import Defs._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.hive.HiveContext
import com.typesafe.config._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/***********************************************************

// Example spark application that handles ingestion of impression data
object ExampleApp {

  // this is only an example function, not meant to be run
  def main(args: Array[String]) = {

    // parse arguments:
    if(args.size < 4) throw new Exception("Must specify 4 arguments: inputFilePath, configFilePath, avroSchemaHDFSPath and EnrichedHDFSOutputDir")
    val inputFilePath = args(0)
    val configFilePath = args(1)
    val avroSchemaHDFSPath = args(2)
    val enrichedOutputBaseDir = args(3)

    // initialise spark context
    val conf = new SparkConf().setAppName("ExampleApp")
    val sc = new SparkContext(conf)

    try {

      val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

      val config = sc.textFile(configFilePath).collect().mkString("")

      println("===========================================================")
      println("===========================================================")
      println("===========================================================")
      println("config json = "+pretty(render(parse(config))))
      println("===========================================================")

      //SELECT ods_affiliate_id, affiliate_id from affiliate (SAVE for now)
      // gives: 
      // ods aff id(int)  aff id (str)
      // 9301129          9301129O
      // 7145092          7145092O   
      // 8944533          8944533O
      // note:
      // `affiliate_id` varchar(11), 
      // `ods_affiliate_id` int, 

      val enrichedRDD: RDD[Map[String, String]] = 
        ActionEngine.processDir(
          new URI(inputFilePath),
          config,
          sc,
          hiveContext = Option(hiveContext),
          actionFactory = new ActionFactory(new ExampleCustomActionCreator))

      // todo check that enrichedRDD has same 'schema' as avro schema

      // Output the data.  (todo maybe this directory formatting could be made a module)
      val format = new SimpleDateFormat("y-MM-dd")
      val dateArray = format.format(Calendar.getInstance().getTime()).split("-")
      val outputDir = enrichedOutputBaseDir+"/year="+dateArray(0)+"/month="+dateArray(1)+"/day="+dateArray(2)
      AvroOutputWriter.write(enrichedRDD, outputDir, avroSchemaHDFSPath, sc)
      println("%%%%%%%%%%%%%%%%%%%% enrichedOutputBaseDir = "+enrichedOutputBaseDir)
    }
    finally {
      // terminate spark context
      sc.stop()
    }
  }
}

******************************************************************/
