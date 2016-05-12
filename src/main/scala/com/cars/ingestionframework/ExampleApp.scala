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

  /** Run the enrichment process
    *
    */
  def enrich(
    sc: SparkContext, 
    configFilePath: String, 
    inputFilePath: String ):
    List[Map[String, String]]= {

    //val textFileRDD = sc.textFile("build.sbt")
    //println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% num lines: "+textFileRDD.count())
    //println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% top line : "+textFileRDD.collect.head)

    // Parse the config.  Creates a list of SourceActions.
    val actions: List[SourceAction] = new ActionFactory().create(configFilePath)
    
    // (strip the newlines - TODO - what does real input look like?)
    val oneLineInput = scala.io.Source.fromFile(inputFilePath).getLines.mkString.filter( _ != '\n' )
    
    // Get the input file 
    //val inputRDD = sc.textFile(inputFilePath) // TODO - restore
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
    
    var allEnriched = List.empty[Map[String, String]]

    //// for every impression, perform all actions from config file.
    //flattenedImpressionsRDD.foreach{ ast => 
    //
    //  // This is the output map, to be filled with the results of validation & 
    //  // enrichment actions.  Later it will be converted to Avro format and saved.
    //  var enrichedMap = new HashMap[String, String]
    //
    //  // for every field in this impression data:
    //  ast.children.foreach{ field => 
    //
    //    // Search in the configuration to find the SourceAction for this field.
    //    val sourceAction = actions.filter( _.source == field).headOption
    //
    //    if(sourceAction.nonEmpty) {
    //      // Found it. Call performActions.
    //      val mapAddition = sourceAction.get.perform(sourceAction.get.source, ast, enrichedMap)
    //      // TODO - pass in list (get list from source in config)
    //
    //      // Then merge in the results.
    //      enrichedMap //TODOTODO = MapUtil.merge(mapAddition, enrichedMap)
    //    }
    //    else {
    //      // TODO - handle the case where there is no configuration for this field.
    //      // Right now it is a no-op (the field is filtered).  However, this 
    //      // could default to 'simple-copy' behavior (or something else) for
    //      // ease of use.
    //    }
    //  }
    //
    //  //// Now write out the enriched record. (TODO - is there a better way to do this?  This will write one file per enriched record.)
    //  //AvroWriter.appendEnrichedToFile(enrichedMap, "hdfs://some/path/to/enriched/data")
    //
    //  // (For now, just return the enriched data)
    //  allEnriched = MapUtil.merge(enrichedMap, allEnriched)
    //}
    //
    allEnriched
  }

  /** 
    *
    */
  def main(args: Array[String]) = {
    
    // TODO - spark stuff - get the spark context, etc.
          
    // initialise spark context
    val conf = new SparkConf().setAppName("ExampleApp").setMaster("local[1]")
    val sc = new SparkContext(conf)
    
    try {
      val listOfEnriched: List[Map[String, String]] = enrich(
        sc, 
        configFilePath = "./src/test/resources/testconfig-integration.json", 
        inputFilePath = "./src/test/resources/input-integration.json")

      // TODO check listOfEnriched

    }
    finally {
      // terminate spark context
      sc.stop()
    }
    
  }

}

// tech TODO:

// 1 - how to write each enriched record to avro in spark context? (Nageswar)
// 2 - create ActionListFactory and interface
