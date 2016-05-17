package com.cars.ingestionframework.exampleapp

import com.cars.ingestionframework._

import scala.collection.immutable.HashMap
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import Defs._
import org.apache.avro.Schema
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import scala.io.Source
import com.databricks.spark.avro._


// Example spark application that handles ingestion of impression data
object ExampleApp {

  /** Run the enrichment process
    *
    */
  def enrich(
    sc: SparkContext, 
    configFilePath: String, 
    inputFilePath: String ):
    List[Map[String, String]]= {

    // Parse the config.  Creates a list of SourceActions.
    val actions: List[SourceAction] = (new ActionFactory).createSourceActions(configFilePath)
    
    // (strip the newlines - TODO - what does real input look like?)
    val oneLineInput = scala.io.Source.fromFile(inputFilePath).getLines.mkString.filter( _ != '\n' )
    
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
    val conf = new SparkConf().setAppName("ExampleApp").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val avroSchemaHDFSPath = "./src/test/resources/avroScehma.avsc" // AvroSceham HDFS path as third argument
    val schemaParser = new Schema.Parser // create Instance Avro Schema Parser
    val avroSchema = schemaParser.parse(avroSchemaHDFSPath).asInstanceOf[StructType] // Parse AvroSchema as Instance of StructType

    try {
      val listOfEnriched: List[Map[String, String]] = enrich(
        sc, 
        configFilePath = "./src/test/resources/testconfig-integration.json", 
        inputFilePath = "./src/test/resources/input-integration.json")

        var RowsBuffer = new ListBuffer[Row]

        //Loop through enriched record fields
        for(i <- listOfEnriched) {
            //convert all the fields' values to a sequence
             RowsBuffer += Row.fromSeq(i.values.toSeq)
        }

      //Converts RowsBuffer to a List[Row]
      //Converts all Scala List[Row] to util.List[Row] of Java and provide avroScehma from HDFS path
      val DataFrame = sqlContext.createDataFrame(RowsBuffer.toList.asJava,avroSchema)

      val EnrichedOutputHDFS = "./target/output"
      DataFrame.write.format("com.databricks.spark.avro").save(EnrichedOutputHDFS)

      println("listOfEnriched = "+listOfEnriched)

    }
    finally {
      // terminate spark context
      sc.stop()
    }
    
  }

  /*def getAvroSchema(HDFSPath : String): Array[StructField] =
  {
    val x: Array[StructField] = new Array[StructField](1)

    return x
  }*/

  /*def getDimensionsBroadcast(path : String): Broadcast[Map[String, (String, String, String)]] =
  {
    val result_rdd = sc.broadcast(sc.textFile(path).map(_.split(',')).map(y => (y(0), (y(1),y(2),y(3)))).collectAsMap().toMap)
    return result_rdd
  }*/

}

// tech TODO:

// 1 - how to write each enriched record to avro in spark context? (Nageswar)
// 2 - create ActionListFactory and interface
