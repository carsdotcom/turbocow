package com.cars.turbocow

import com.cars.turbocow.actions.Lookup
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
    *
    * @param inputDir should be local file or HDFS Directory
    * @param config configuration file that describes how to process the input data
    * @param sc SparkContext
    * @param hiveContext the hive context, if any
    * @param actionFactory the factory you want to utilize for creating Action objects
    * 
    * @return RDD of enriched data records, where each record is a key-value map.
    */
  def process(
    inputDir: String,
    config: String,
    sc: SparkContext,
    hiveContext : Option[HiveContext] = None,
    actionFactory: ActionFactory = new ActionFactory ): 
    RDD[Map[String, String]] = {

    // Parse the config.  Creates a list of Items.
    val driverItems = actionFactory.createItems(config)
    val items: Broadcast[List[Item]] = sc.broadcast(driverItems)

    // Cache all the tables as specified in the items, then broadcast
    val tableCachesDriver: Map[String, TableCache] = cacheTables(driverItems, hiveContext)
    val tableCaches = sc.broadcast(tableCachesDriver)

    // Get the input file
    val inputJsonRDD = sc.textFile(inputDir)

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

  /** Scan a list of Items and return a map of dbAndTable to list of 
    * all Lookup actions on that table.
    * 
    * @param  items list to search through for actions needing cached lookup tables
    * @return list of CachedLookupRequirements objects, one per table
    */
  def getAllLookupRequirements(items: List[Item]):
    List[CachedLookupRequirement] = {

    val allReqList: List[ (String, CachedLookupRequirement) ] = items.flatMap{ item =>
      val actionReqs: List[CachedLookupRequirement] = item.actions.flatMap{ action =>
        action.getLookupRequirements
      }
      actionReqs
    }.map{ req => (req.dbTableName, req) }

    val allReqsMap: Map[ String, List[CachedLookupRequirement]] = 
      allReqList.groupBy{ _._1 }.map{ case(k, list) => (k, list.map{ _._2 } ) }

    // combine to form one map of dbTableName to requirements.
    // I feel like this last bit could be simplified.  TODO
    val combinedRequirements: Map[String, CachedLookupRequirement] = allReqsMap.map{ case (dbTableName, reqList) =>
      ( dbTableName,
        reqList.reduceLeft{ (combined, e) =>
          combined.combineWith(e)
        } 
      )
    }

    combinedRequirements.toList.map{ _._2 }
  }

  /** Create local caches of all of the tables in the action list.
    * 
    * @return map of dbTableName to TableCache
    * 
    */
  def cacheTables(
    items: List[Item],
    hiveContext: Option[HiveContext]): 
    Map[String, TableCache] = {

    val allRequirements: List[CachedLookupRequirement] = getAllLookupRequirements(items)

    // Return map of all table names to HiveTableCaches.
    allRequirements.map{ req => (
      req.dbTableName, 
      HiveTableCache(
        hiveContext,
        req.dbTableName,
        keyFields = req.keyFields,
        fieldsToSelect = req.allNeededFields,
        req.jsonRecordsFile
      )
    )}.toMap
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

