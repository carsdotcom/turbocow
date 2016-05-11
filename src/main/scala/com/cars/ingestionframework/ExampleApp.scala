package com.cars.ingestionframework.exampleapp

import com.cars.ingestionframework._
import scala.collection.immutable.HashMap

// Example spark application that handles ingestion of impression data
object ExampleApp {

  def main(args: Array[String]) = {
    
    // todo - spark stuff - get the spark context, etc.

    // Parse the config.  Create a IngestionConfigurationConfig case class.
    val config = ConfigParser.parse("hdfs://Path/To/ImpressionConfig")

    // create map of source to an action list
    val actions: List[SourceAction] = ActionListFactory.create(config)

    // Get the input file
    val inputRDD = sc.textFile("hdfs://input/file/path")

    // parse the input json data 
    var allImpressionsRDD = inputRDD.map( jsonRecord => {
      implicit val formats = DefaultFormats
      parse(jsonRecord)
    })

    // merge them so activityMap & metaData are together
    val flattenedImpressionsRDD = allImpressionsRDD.map{ ast => 
      (ast \ "metaData") merge (ast \ "activityMap")
    }
  
    // for every impression, perform all actions from config file.
    flattenedImpressionsRDD.foreach{ ast => 
  
      // This is the output map, to be filled with the results of validation & 
      // enrichment actions.  Later it will be converted to Avro format and saved.
      var enrichedMap: Map[String, String] = new HashMap[String, String]

      ast.children.foreach{ field => 

        // search in the configuration to find the SourceAction for this field.
        val sourceAction = actions.filter( _.source == field).headOption
        if(sourceAction.nonEmpty) {
          // Found it. Call performActions.
          val mapAddition = sourceAction.performActions(field, ast, enrichedMap)
          // todo - pass in list (get list from source in config)

          // Then merge in the results.
          enrichedMap = enrichedMap.merged(mapAddition) // todo test, specify merge func
        }
        else {
          // todo - handle the case where there is no configuration for this field.
        }
      }

      // Now write out the enriched record.
      AvroWriter.appendEnrichedToFile(enrichedMap, "hdfs://some/path/to/enriched/data")

      // todo - is there a better way to do this?  This will write one file per enriched record.
    }
  }

}

// tech todo:

// 1 - how to write each enriched record to avro in spark context? (Nageswar)
// 2 - create ActionListFactory and interface
