package com.cars.ingestionframework.exampleapp

import com.cars.ingestionframework._
import scala.collection.immutable.HashMap

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// Example spark application that handles ingestion of impression data
object ExampleApp {

  /** Run the spark application
    *
    */
  def run(
    sc: SparkContext, 
    configFilePath: String, 
    inputFilePath: String ) = {

    //val textFileRDD = sc.textFile("build.sbt")
    //println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% num lines: "+textFileRDD.count())
    //println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% top line : "+textFileRDD.collect.head)

    // Parse the config.  Creates a list of SourceActions.
    val actions: List[SourceAction] = new ActionFactory().create(configFilePath)
    
    // (strip the newlines - todo - what does real input look like?)
    val oneLineInput = scala.io.Source.fromFile(inputFilePath).getLines.mkString.filter( _ != '\n' )
    
    // Get the input file 
    //val inputRDD = sc.textFile(inputFilePath) // todo - restore
    val inputRDD = sc.parallelize(List(oneLineInput))
    println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX inputRDD = ")
    inputRDD.collect().foreach(i => println("NEXT: "+i))
    println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX ")
    
    // use default formats for parsing
    implicit val jsonFormats = org.json4s.DefaultFormats
    
    // parse the input json data 
    var allImpressionsRDD = inputRDD.map( jsonRecord => {
      parse(jsonRecord)
    })
    
    // merge them so activityMap & metaData are together
    val flattenedImpressionsRDD = allImpressionsRDD.map{ ast => 
      (ast \ "metaData") merge (ast \ "activityMap")
    }
    
    println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% flattenedImpressionsRDD = ")
    flattenedImpressionsRDD.collect().foreach(i => println("NEXT: "+pretty(render(i))))
    println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")    
    
    //// for every impression, perform all actions from config file.
    //flattenedImpressionsRDD.foreach{ ast => 
    //
    //  // This is the output map, to be filled with the results of validation & 
    //  // enrichment actions.  Later it will be converted to Avro format and saved.
    //  var enrichedMap: Map[String, String] = new HashMap[String, String]
    //
    //  ast.children.foreach{ field => 
    //
    //    // search in the configuration to find the SourceAction for this field.
    //    val sourceAction = actions.filter( _.source == field).headOption
    //    if(sourceAction.nonEmpty) {
    //      // Found it. Call performActions.
    //      val mapAddition = sourceAction.performActions(field, ast, enrichedMap)
    //      // todo - pass in list (get list from source in config)
    //
    //      // Then merge in the results.
    //      enrichedMap = enrichedMap.merged(mapAddition) // todo test, specify merge func
    //    }
    //    else {
    //      // todo - handle the case where there is no configuration for this field.
    //    }
    //  }
    //
    //  // Now write out the enriched record.
    //  AvroWriter.appendEnrichedToFile(enrichedMap, "hdfs://some/path/to/enriched/data")
    //
    //  // todo - is there a better way to do this?  This will write one file per enriched record.
    //}
  }

  /** 
    *
    */
  def main(args: Array[String]) = {
    
    // todo - spark stuff - get the spark context, etc.
          
    // initialise spark context
    val conf = new SparkConf().setAppName("ExampleApp").setMaster("local[1]")
    val sc = new SparkContext(conf)
    
    try {
      run(
        sc, 
        configFilePath = "./src/test/resources/testconfig-integration.json", 
        inputFilePath = "./src/test/resources/input-integration.json")
    }
    finally {
      // terminate spark context
      sc.stop()
    }
    
  }

}

// tech todo:

// 1 - how to write each enriched record to avro in spark context? (Nageswar)
// 2 - create ActionListFactory and interface
