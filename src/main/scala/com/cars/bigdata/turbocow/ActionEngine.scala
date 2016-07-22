package com.cars.bigdata.turbocow

import java.net.URI

import com.cars.bigdata.turbocow.actions.Lookup
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.immutable.HashMap

object ActionEngine
{

  /** Process a set of input files by running the actions specified in the config file.
    * This is the process function that will mostly be called.
    *
    * @param inputDir should be local file or HDFS Directory (start with "hdfs://"
    *        for HDFS files or "/" for local files for testing)
    * @param config configuration file that describes how to process the input data
    * @param sc SparkContext
    * @param hiveContext the hive context, if any
    * @param actionFactory the factory you want to utilize for creating Action objects
    * 
    * @return RDD of enriched data records, where each record is a key-value map.
    */
  def processDir(
    inputDir: java.net.URI,
    config: String,
    sc: SparkContext,
    hiveContext : Option[HiveContext] = None,
    actionFactory: ActionFactory = new ActionFactory ): 
    RDD[Map[String, String]] = {

    // Get the input file
    val inputJsonRDD = sc.textFile(inputDir.toString)

    processJsonRDD(inputJsonRDD, config, sc, hiveContext, actionFactory)
  }

  /** Process a set of JSON strings rather than reading from a directory.
    * This will mostly be used during testing.
    *
    * @param inputJSON a sequence of json strings, one JSON record per element.
    * @param config configuration file that describes how to process the input data
    * @param sc SparkContext
    * @param hiveContext the hive context, if any
    * @param actionFactory the factory you want to utilize for creating Action objects
    * 
    * @return RDD of enriched data records, where each record is a key-value map.
    */
  def processJsonStrings(
    inputJson: Seq[String],
    config: String,
    sc: SparkContext,
    hiveContext : Option[HiveContext] = None,
    actionFactory: ActionFactory = new ActionFactory ): 
    RDD[Map[String, String]] = {

    // Create RDD from the inputJson strings
    val inputJsonRDD = sc.parallelize(inputJson)

    processJsonRDD(inputJsonRDD, config, sc, hiveContext, actionFactory)
  }

  /** Process a set of JSON strings rather than reading from a directory.
    * This will likely never be called except from the two above process functions.
    *
    * @param inputJSONRDD an RDD of JSON Strings to process
    * @param config configuration file that describes how to process the input data
    * @param sc SparkContext
    * @param hiveContext the hive context, if any
    * @param actionFactory the factory you want to utilize for creating Action objects
    * 
    * @return RDD of enriched data records, where each record is a key-value map.
    */
  def processJsonRDD(
    inputJsonRDD: RDD[String],
    config: String,
    sc: SparkContext,
    hiveContext : Option[HiveContext] = None,
    actionFactory: ActionFactory = new ActionFactory ): 
    RDD[Map[String, String]] = {

    // Parse the config.  Creates a list of Items.
    val driverItems = actionFactory.createItems(config)
    val items: Broadcast[List[Item]] = sc.broadcast(driverItems)

    // Cache all the tables as specified in the items, then broadcast
    val tableCachesDriver: Map[String, TableCache] = 
      HiveTableCache.cacheTables(driverItems, hiveContext)
    val tableCaches = sc.broadcast(tableCachesDriver)

    // parse the input json data
    val flattenedImpressionsRDD = inputJsonRDD.map( jsonString => {
      val ast = parse(jsonString)
      // 'flatten' the json so activityMap & metaData's members are together at the
      // same level:
      // todo (open) make this configurable in the JSON.
      (ast \ "md") merge (ast \ "activityMap")
    })

    // for every impression, perform all actions from config file.
    flattenedImpressionsRDD.map{ ast =>

      val actionContext = ActionContext(tableCaches.value)

      // This is the output map, to be filled with the results of validation &
      // enrichment actions.  Later it will be converted to Avro format and saved.
      var enrichedMap: Map[String, String] = new HashMap[String, String]

      // For every item, process its actionlist.
      items.value.foreach{ item =>
        val result = item.perform(ast, enrichedMap, actionContext)
        // Note that the 'stopProcessingActionList' field is ignored and not
        // passed on to the next action list.
        enrichedMap = enrichedMap ++ result.enrichedUpdates
      }

      // We are rejecting this record
      if (actionContext.rejectionReasons.nonEmpty) {
        // add any rejection reasons to the enriched record
        enrichedMap = enrichedMap + ("reasonForReject"-> actionContext.rejectionReasons.toString)

        // copy in all the fields from the input record.
        enrichedMap = addAllFieldsToEnriched(ast, enrichedMap)
      }

      // (For now, just return the enriched data)
      enrichedMap
    }
  }

  /** Add all fields from input record (as parsed AST) to the enriched map.
    * 
    * @param inputRecordAST the input record as parsed JSON AST
    * @param enrichedMap the current enriched record
    * @return the new enriched map
    */
  def addAllFieldsToEnriched(
    inputRecordAST: JValue, 
    enrichedMap: Map[String, String]): 
    Map[String, String] = {

    val inputMap: Map[String, Option[String]] = inputRecordAST match { 
      case JObject(o) => o.toMap.map{ case (k,v) => (k, JsonUtil.extractOptionString(v)) }
      case _ => throw new Exception("The input record must be a JSON object (not an array or other type).")
      // TODO double check the ALS code so that it always outputs an object.
    }

    val inputToEnrichedMap = inputMap.flatMap{ case(k, v) => 
      // if key is not in the enriched map, add it.
      val enrichedOpt = enrichedMap.get(k)
      if (enrichedOpt.isEmpty) { // not found
        Some((k, v.getOrElse("")))
      }
      else { // otherwise don't add it
        None
      }
    } 

    // return the merged maps
    inputToEnrichedMap ++ enrichedMap
  }
}

