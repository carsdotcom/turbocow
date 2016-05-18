package com.cars.ingestionframework.exampleapp

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


// Example spark application that handles ingestion of impression data
object ExampleApp {

  /** Run the enrichment process
    *
    */
  def enrich(
    sc: SparkContext, 
    configFilePath: String, 
    inputFilePath: String, hiveContext: HiveContext ):
    List[Map[String, String]]= {

    // Parse the config.  Creates a list of SourceActions.
    val actions: List[SourceAction] = new ActionFactory().create(configFilePath, hiveContext, sc)
    
    // (strip the newlines - TODO - what does real input look like?)
    //val oneLineInput = scala.io.Source.fromFile(inputFilePath).getLines.mkString.filter( _ != '\n' )
    val oneLineInput = sc.textFile(inputFilePath).collect().mkString("")
    
    // Get the input file 
    //val inputRDD = sc.textFile(inputFilePath) // TODO - restore
    val inputRDD = sc.parallelize(List(oneLineInput))
    
    // use default formats for parsing
    implicit val jsonFormats = org.json4s.DefaultFormats
    
    // parse the input json data 
    val allImpressionsRDD = inputRDD.map( jsonRecord => {
      parse(jsonRecord)
    })
    
    // merge them so activityMap & metaData are together
    val flattenedImpressionsRDD = allImpressionsRDD.map{ ast => 
      (ast \ "metaData") merge (ast \ "activityMap")
    }
    
    // for every impression, perform all actions from config file.
    // TODOTODO - I had to collect this RDD before running this... not sure why yet...
    flattenedImpressionsRDD.collect.map{ ast =>
    
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
          val mapAddition = sourceAction.get.perform(sourceAction.get.source, ast, enrichedMap)
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
    
      //// Now write out the enriched record. (TODO - is there a better way to do this?  This will write one file per enriched record.)
      //AvroWriter.appendEnrichedToFile(enrichedMap, "hdfs://some/path/to/enriched/data")
    
      // (For now, just return the enriched data)
      enrichedMap
    }.toList
  }

  /** run this.  this is only really for manually testing.  See ExampleAppSpec 
    * for detailed integration tests.
    *
    */
  def main(args: Array[String]) = {
    
    // initialise spark context
    val conf = new SparkConf().setAppName("ExampleApp")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val avroSchemaHDFSPath = "hdfs://ch1-hadoop-ns:8020/tmp/spark_jar/document.json" // AvroSceham HDFS path as third argument
    val schema = getAvroSchema(avroSchemaHDFSPath,sc)

    val structTypeSchema = StructType(schema(0).map(column => StructField(column,StringType,true))) // Parse AvroSchema as Instance of StructType

    try {
      val listOfEnriched: List[Map[String, String]] = enrich(
        sc, 
        configFilePath = args(1),
        inputFilePath = args(0), hiveContext)

      var rowsBuffer = new ArrayBuffer[Row]

        //Loop through enriched record fields
      for(i <- listOfEnriched) {
            //convert all the fields' values to a sequence
             rowsBuffer += Row.fromSeq(i.values.toSeq)
        }

      //Converts RowsBuffer to a RDD[Row]
      val enrichedRows = sc.parallelize(rowsBuffer)

      //create a dataframe of RDD[row] and Avro schema
      val dataFrame = sqlContext.createDataFrame(enrichedRows,structTypeSchema)

      val EnrichedOutputHDFS = "./target/output"
      dataFrame.write.format("com.databricks.spark.avro").save(EnrichedOutputHDFS)

      println("listOfEnriched = "+listOfEnriched)

    }
    finally {
      // terminate spark context
      sc.stop()
    }
    
  }

  def getAvroSchema(HDFSPath : String, sc: SparkContext): Array[Array[String]] = {

    val jsonRDD = sc.textFile(HDFSPath)
    val oneLineAvroSchema = jsonRDD.collect().mkString("")
    val lineRDD = sc.parallelize(List(oneLineAvroSchema))
    val parseJsonRDD = lineRDD.map(record => parse(record))
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

// tech TODO:

// 1 - how to write each enriched record to avro in spark context? (Nageswar) - Done
// 2 - create ActionListFactory and interface
