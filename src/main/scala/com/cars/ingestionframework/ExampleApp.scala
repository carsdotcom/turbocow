package com.cars.ingestionframework.exampleapp

import java.io.Serializable
import java.lang.{Boolean, Double, Long}
import java.util
import java.util.Map.Entry
import java.util.concurrent.TimeUnit

import com.cars.ingestionframework._

import scala.collection.immutable.HashMap
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


// Example spark application that handles ingestion of impression data
object ExampleApp {

  /** Run the enrichment process
    *
    * @param sc
    * @param config
    * @param inputDir should be local file or HDFS Directory
    * @param hiveContext
    * @param actionFactory
    */
  def enrich(
              sc: SparkContext,
              config: String,
              inputDir: String,
              hiveContext : Option[HiveContext] = None,
              actionFactory: ActionFactory = new ActionFactory ):
    RDD[Map[String, String]]= {

    // Parse the config.  Creates a list of SourceActions.
    val actions: List[SourceAction] = actionFactory.createSourceActions(config)

    // Get the input file
    val inputJsonRDD = sc.textFile(inputDir)

    // parse the input json data
    val flattenedImpressionsRDD = inputJsonRDD.map( jsonString => {
      // use default formats for parsing
      implicit val jsonFormats = org.json4s.DefaultFormats
      val ast = parse(jsonString)
      // 'flatten' the json so activityMap & metaData's members are together at the
      // same level:
      (ast \ "md") merge (ast \ "activityMap")
    })

    val actionCtx = ActionContext(hiveContext)

    // create lookup table Dataframe for reference
    //val query = s"""select * FROM dw_dev.local_offer"""
    //val query = s"""select ods_local_offer_id, local_offer_opt_in_ind FROM dw_dev.local_offer"""
    //val lookupDF = hiveContext.get.sql(query)
    //println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ 1 lookupDF="+lookupDF)
    //println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ 2 lookupDF.show=")
    //lookupDF.show
    //println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ 2.1 lookupDF.show done")
    //
    //;{
    //  val query = s"""select * FROM dw_dev.local_offer"""
    //  val df = hiveContext.get.sql(query).where(s"""local_offer_id='2018434' """)
    //  println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ 3 df="+df)
    //  println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ 4 df.show=")
    //  df.show
    //  println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ 4.1 df.show done")
    //}
    //val lookupBroadcast = sc.broadcast(lookupDF)

    // for every impression, perform all actions from config file.
    flattenedImpressionsRDD.map{ ast =>

      // This is the output map, to be filled with the results of validation &
      // enrichment actions.  Later it will be converted to Avro format and saved.
      var enrichedMap: Map[String, String] = new HashMap[String, String]

      // extract all the fields: (TODO write helper for this)
      val fields = ast match {
        case j: JObject => j.values
        case _ => throw new Exception("couldn't find values in record: "+ast); // todo - reject this
      }

      /** Process a field - called below, uses wrapped function vars.
        * This was necessary because of the match on (key, null) below (for "fields");
        * so this code is not duplicated.
        */
      def processField(key: String):
        StringMap = {

        // Search in the configuration to find the SourceAction for this field.
        val sourceAction = actions.filter( _.source.contains(key) ).headOption
        // (TODO check with Ramesh to see if the input source fields are always unique in config file)

        if(sourceAction.nonEmpty) {
          // Found it. Call performActions.
          println("PPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPerform")
          val mapAddition = sourceAction.get.perform(sourceAction.get.source, ast, enrichedMap, actionCtx)
          // TODO - pass in list (get list from source in config)

          // Then merge in the results.
          mapAddition
        }
        else {
          // TODO - handle the case where there is no configuration for this field.
          // Right now it is a no-op (the field is filtered).  However, this
          // could default to 'simple-copy' behavior (or something else) for
          // ease of use.
          Map.empty[String, String] // don't add anything
        }
      }

      // for every field in this impression data:
      fields.foreach{
        case (key: String, value: Any) => enrichedMap = enrichedMap ++ processField(key)
        case (key: String, null) => enrichedMap = enrichedMap ++ processField(key)
      }

      // (For now, just return the enriched data)
      enrichedMap
    }
  }

  /** run this.  this is only really for manually testing.  See ExampleAppSpec
    * for detailed integration tests.
    *
    */
  def main(args: Array[String]) = {

    // parse arguments:
    if(args.size < 4) throw new Exception("Must specify 4 arguments: inputFilePath, configFilePath, avroSchemaHDFSPath and EnrichedHDFSOutputDir")
    val inputFilePath = args(0)
    val configFilePath = args(1)
    val avroSchemaHDFSPath = args(2)
    val enrichedOutputHDFS = args(3)

    // initialise spark context
    //val conf = new SparkConf().setAppName("ExampleApp").setMaster("local[1]")
    val conf = new SparkConf().setAppName("ExampleApp")
    val sc = new SparkContext(conf)
    val schema = getAvroSchema(avroSchemaHDFSPath,sc)

    val structTypeSchema = StructType(schema(0).map(column => StructField(column, StringType, true))) // Parse AvroSchema as Instance of StructType

    try {

      val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

      val config = sc.textFile(configFilePath).collect().mkString("")

      println("===========================================================")
      println("===========================================================")
      println("===========================================================")
      println("config json = "+config)
      println("===========================================================")

      val enrichedRDD: RDD[Map[String, String]] = enrich(
        sc,
        config,
        inputDir = inputFilePath,
        hiveContext = Option(hiveContext),
        new ActionFactory(new ExampleCustomActionCreator))

      // todo check that enrichedRDD has same 'schema' as avro schema

      //Loop through enriched record fields
      val rowRDD = enrichedRDD.map( i =>
        //convert all the fields' values to a sequence
         Row.fromSeq(i.values.toSeq)
      )

      //create a dataframe of RDD[row] and Avro schema
      val sqlContext = new SQLContext(sc)
      val dataFrame = sqlContext.createDataFrame(rowRDD, structTypeSchema)

      dataFrame.write.format("com.databricks.spark.avro").save(enrichedOutputHDFS)
    }
    finally {
      // terminate spark context
      sc.stop()
    }
  }

  /**Process AvroScehema from HDFS
 *
    * @param hdfsPath
    * @param sc SparkContext
    */
  def getAvroSchema(hdfsPath : String, sc: SparkContext): Array[Array[String]] = {

    val jsonRDD = sc.textFile(hdfsPath)
    val oneLineAvroSchema = jsonRDD.collect().mkString("")
    val lineRDD = sc.parallelize(List(oneLineAvroSchema))
    val parseJsonRDD = lineRDD.map { record =>
      implicit val jsonFormats = org.json4s.DefaultFormats
      parse(record)
    }
    val fieldsList = parseJsonRDD.collect().map(eachline => {

      implicit val formats = org.json4s.DefaultFormats

      //collect fields array from avro schema
      val fieldsArray = (eachline \ "fields").children

      //make array from all field names from avro schema
      val b = fieldsArray.map(eachChild =>
        (eachChild \ "name").extract[String])
      b.toArray
    })
    fieldsList
  }

}
