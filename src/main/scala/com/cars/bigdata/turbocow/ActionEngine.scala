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
    actionFactory: ActionFactory = new ActionFactory,
    initialScratchPad: ScratchPad = new ScratchPad):
    RDD[Map[String, String]] = {

    // Get the input file
    val inputJsonRDD = sc.textFile(inputDir.toString)

    processJsonRDD(inputJsonRDD, config, sc, hiveContext, actionFactory, initialScratchPad)
  }

  /** Process a set of JSON strings rather than reading from a directory.
    * This will mostly be used during testing.
    *
    * @param inputJson a sequence of json strings, one JSON record per element.
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
    actionFactory: ActionFactory = new ActionFactory,
    initialScratchPad: ScratchPad = new ScratchPad):
    RDD[Map[String, String]] = {

    // Create RDD from the inputJson strings
    val inputJsonRDD = sc.parallelize(inputJson)

    processJsonRDD(inputJsonRDD, config, sc, hiveContext, actionFactory, initialScratchPad)
  }

  /** Process a set of JSON strings rather than reading from a directory.
    * This will likely never be called except from the two above process functions.
    *
    * @param inputJsonRDD an RDD of JSON Strings to process
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
    actionFactory: ActionFactory = new ActionFactory,
    initialScratchPad: ScratchPad = new ScratchPad):
    RDD[Map[String, String]] = {

    // Parse the config.  Creates a list of Items.
    val items = actionFactory.createItems(config)
    val itemsBC: Broadcast[List[Item]] = sc.broadcast(items)

    // Cache all the tables as specified in the items, then broadcast
    val tableCaches: Map[String, TableCache] =
      HiveTableCache.cacheTables(items, hiveContext)
    val tableCachesBC = sc.broadcast(tableCaches)

    val initialScratchPadBC = sc.broadcast(initialScratchPad)

    // parse the input json data
    val flattenedImpressionsRDD = inputJsonRDD.map( jsonString => {
      val ast = parse(jsonString)
      // 'flatten' the json so activityMap & metaData's members are together at the
      // same level:
      // todo (open) make this configurable in the JSON.
      val mergedAST = {
        //val theRest = ast.children.flatMap{ jval =>
        //  jval match {
        //    case JNothing => 
        //  } 
        //}
        val md = (ast \ "md").toOption 
        val activityMap = (ast \ "activityMap").toOption
        if (md.nonEmpty && activityMap.nonEmpty) md.get merge activityMap.get
        else if (md.nonEmpty) md.get
        else if (activityMap.nonEmpty) activityMap.get
        else ast
        // TODOTODO what if one or the other missing; what about the rest of the stuff?
      }
      mergedAST
    })

    // for every impression, perform all actions from config file.
    flattenedImpressionsRDD.map{ ast =>
      try {
        processRecord(ast, itemsBC.value, initialScratchPadBC.value, tableCachesBC.value)
      }
      catch {
        case _: Throwable => Map.empty[String, String]
      }
    } //.filter{ m => m != Map.empty[String, String] }
  }

  /** Process one JSON record.  Called from processJsonRDD and tests.
    */
  protected [turbocow] 
  def processRecord(
    record: JValue,
    configItems: List[Item],
    initialScratchPad: ScratchPad = new ScratchPad,
    tableCaches: Map[String, TableCache] = Map.empty[String, TableCache]): 
    Map[String, String] = {

    val actionContext = ActionContext(tableCaches, scratchPad = initialScratchPad)

    // This is the output map, to be filled with the results of validation &
    // enrichment actions.  Later it will be converted to Avro format and saved.
    var enrichedMap: Map[String, String] = new HashMap[String, String]

    // For every item, process its actionlist.
    configItems.foreach { item =>
      val result = item.perform(record, enrichedMap, actionContext)
      // Note that the 'stopProcessingActionList' field is ignored and not
      // passed on to the next action list.
      enrichedMap = enrichedMap ++ result.enrichedUpdates
    }

    // (For now, just return the enriched data)
    enrichedMap
  }


}

