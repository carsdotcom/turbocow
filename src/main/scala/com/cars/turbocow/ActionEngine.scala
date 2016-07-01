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
    val driverItems = actionFactory.createSourceActions(config)
    val items: Broadcast[List[SourceAction]] = sc.broadcast(driverItems)
    //println("sourceActions = "+sourceActions)

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

  /** Scan a list of SourceActions and return a map of dbAndTable to list of 
    * all Lookup actions on that table.
    * 
    * @param  sourceActions to search through for Lookup actions
    * @return map of dbAndTable name to list of Lookup actions that utilize that 
    *                table
    */
  def getAllLookupActions(sourceActions: List[SourceAction]): 
    Map[String, List[Lookup]] = {
  
    val lookupsPerTable = sourceActions.flatMap{ sa => 
    
      sa.actions.flatMap{ 
    
        case lookup: Lookup => lookup.fromFile match { 
          case s: Some[String] => None // for now (Todo implement caching for local files too)
          case None => {
            Some( 
              lookup.dbAndTable, 
              lookup
            )
          }
        }
        case _ => None
      }
    }.groupBy(_._1).mapValues(_.map(_._2))

    lookupsPerTable
  }

  /** Create local caches of all of the tables in the action list.
    * 
    */
  def cacheTables(
    sourceActions: List[SourceAction], 
    hiveContext: Option[HiveContext]): 
    Map[String, TableCache] = {

    val allLookups: Map[String, List[Lookup]] = getAllLookupActions(sourceActions)

    // transform the lookups list into a TableCache.
    val tcMap = allLookups.map{ case(tableAndName, lookupList) =>

      val refLookup = lookupList.head

      // the list of ALL fields to get from the table
      val allFieldsToSelect = lookupList.flatMap{ _.allFields }.distinct

      // the list of all fields to use as 'indexing' keys
      val allIndexFields = lookupList.map{ _.where }.distinct

      // Make sure all the DBs and Tables match:
      lookupList.foreach{ lookup => if (lookup.dbAndTable != refLookup.dbAndTable) throw new Exception(s"the database and table did not match: refLookup.fromDB(${refLookup.fromDB}), refLookup.fromTable(${refLookup.fromTable}), refLookup.dbAndTable(${refLookup.dbAndTable}), lookup.fromDB(${lookup.fromDB}), lookup.fromTable(${lookup.fromTable}), lookup.dbAndTable(${lookup.dbAndTable})")}

      // return a name->TableCache pair)
      (tableAndName, 
        HiveTableCache(
          hiveContext,
          refLookup.fromDB, 
          tableName = refLookup.fromTable, 
          keyFields = allIndexFields,
          fieldsToSelect = allFieldsToSelect
        )
      )
    }
    tcMap
  }


  /** Add all fields from input record (as parsed AST) to the enriched map.
    * 
    * @param inputRecordAST the input record as parsed JSON AST
    * @param enrichedMap the current enriched record
    * 
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

