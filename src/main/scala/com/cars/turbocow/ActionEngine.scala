package com.cars.turbocow

import java.io.Serializable
import java.lang.{Boolean, Double, Long}
import java.text.SimpleDateFormat
import java.util
import java.util.Map.Entry
import java.util.Calendar
import java.text.SimpleDateFormat
import com.cars.turbocow.actions._

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
    RDD[Map[String, String]]= {

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

      // For every action in the list
      items.value.foreach{ action =>
        val result = action.perform(action.source, ast, enrichedMap, actionContext)
        enrichedMap = enrichedMap ++ result.enrichedUpdates
      }

      // We are rejecting this record
      if (actionContext.rejectionReasons.nonEmpty) {
        // add any rejection reasons to the enriched record
        enrichedMap = enrichedMap + ("reasonForReject"-> actionContext.rejectionReasons.toString)
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
    
        case lookup: Lookup => lookup.lookupFile match { 
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
      val allIndexFields = lookupList.map{ _.lookupField }.distinct

      // Make sure all the DBs and Tables match:
      lookupList.foreach{ lookup => if (lookup.dbAndTable != refLookup.dbAndTable) throw new Exception(s"the database and table did not match: refLookup.lookupDB(${refLookup.lookupDB}), refLookup.lookupTable(${refLookup.lookupTable}), refLookup.dbAndTable(${refLookup.dbAndTable}), lookup.lookupDB(${lookup.lookupDB}), lookup.lookupTable(${lookup.lookupTable}), lookup.dbAndTable(${lookup.dbAndTable})")}

      // return a name->TableCache pair)
      (tableAndName, 
        HiveTableCache(
          hiveContext,
          refLookup.lookupDB, 
          tableName = refLookup.lookupTable, 
          keyFields = allIndexFields,
          fieldsToSelect = allFieldsToSelect
        )
      )
    }
    tcMap
  }

}

