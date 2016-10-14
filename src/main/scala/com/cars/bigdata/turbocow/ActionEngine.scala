package com.cars.bigdata.turbocow

import java.net.URI

import com.cars.bigdata.turbocow.actions.{ActionList, Lookup}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.immutable.HashMap
import java.sql.{Connection, DriverManager, Statement}

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
    * @param initialScratchPad at the start of the processing for every record, 
    *        initialize the scratch pad to this.
    * @param jdbcClientConfigs list of JdbcClientConfig objects that the framework
    *        will use to create JDBC clients to use during actions.
    * 
    * @return RDD of enriched data records, where each record is a key-value map.
    */
  def processDir(
    inputDir: java.net.URI,
    config: String,
    sc: SparkContext,
    hiveContext : Option[HiveContext] = None,
    actionFactory: ActionFactory = new ActionFactory,
    initialScratchPad: ScratchPad = new ScratchPad,
    jdbcClientConfigs: Seq[JdbcClientConfig] = Nil):
    RDD[Map[String, String]] = {

    // Get the input file
    val inputJsonRDD = sc.textFile(inputDir.toString)

    processJsonRDD(inputJsonRDD, config, sc, hiveContext, actionFactory, initialScratchPad, jdbcClientConfigs)
  }

  /** Process a set of JSON strings rather than reading from a directory.
    * This will mostly be used during testing.
    *
    * @param inputJson a sequence of json strings, one JSON record per element.
    * @param config configuration file that describes how to process the input data
    * @param sc SparkContext
    * @param hiveContext the hive context, if any
    * @param actionFactory the factory you want to utilize for creating Action objects
    * @param initialScratchPad at the start of the processing for every record, 
    *        initialize the scratch pad to this.
    * @param jdbcClientConfigs list of JdbcClientConfig objects that the framework
    *        will use to create JDBC clients to use during actions.
    * 
    * @return RDD of enriched data records, where each record is a key-value map.
    */
  def processJsonStrings(
    inputJson: Seq[String],
    config: String,
    sc: SparkContext,
    hiveContext : Option[HiveContext] = None,
    actionFactory: ActionFactory = new ActionFactory,
    initialScratchPad: ScratchPad = new ScratchPad,
    jdbcClientConfigs: Seq[JdbcClientConfig] = Nil):
    RDD[Map[String, String]] = {

    // Create RDD from the inputJson strings
    val inputJsonRDD = sc.parallelize(inputJson)

    processJsonRDD(inputJsonRDD, config, sc, hiveContext, actionFactory, initialScratchPad, jdbcClientConfigs)
  }

  /** Process a set of JSON strings rather than reading from a directory.
    * This will likely never be called except from the two above process functions.
    *
    * @param inputJsonRDD an RDD of JSON Strings to process
    * @param config configuration string that describes how to process the input data
    * @param sc SparkContext
    * @param hiveContext the hive context, if any
    * @param actionFactory the factory you want to utilize for creating Action objects
    * @param initialScratchPad at the start of the processing for every record, 
    *        initialize the scratch pad to this.
    * @param jdbcClientConfigs list of JdbcClientConfig objects that the framework
    *        will use to create JDBC clients to use during actions.
    * 
    * @return RDD of enriched data records, where each record is a key-value map.
    */
  def processJsonRDD(
    inputJsonRDD: RDD[String],
    config: String,
    sc: SparkContext,
    hiveContext : Option[HiveContext] = None,
    actionFactory: ActionFactory = new ActionFactory,
    initialScratchPad: ScratchPad = new ScratchPad,
    jdbcClientConfigs: Seq[JdbcClientConfig] = Nil):
    RDD[Map[String, String]] = {

    // Parse the config file and broadcast what we need to.
    val bc = EngineBroadcasts.create(
      config, 
      sc, 
      hiveContext,
      actionFactory, 
      initialScratchPad, 
      jdbcClientConfigs)

    processJsonRDD(inputJsonRDD, bc)
  }

  /** Process a set of JSON strings rather than reading from a directory.
    * This will likely never be called except from the two above process functions.
    *
    * @param inputJsonRDD an RDD of JSON Strings to process
    * @param bc the EngineBroadcasts, which must have been previously broadcast
    *           via the .create method.
    * 
    * @return RDD of enriched data records, where each record is a key-value map.
    * 
    * @todo add similar functions to the other process..() functions above
    */
  def processJsonRDD(
    inputJsonRDD: RDD[String],
    bc: EngineBroadcasts):
    RDD[Map[String, String]] = {

    // parse the input json data
    val flattenedRDD = inputJsonRDD.map( jsonString => {
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
    val enrichedRDD = flattenedRDD.mapPartitions{ iter =>

      val jdbcClients: Map[String, Statement] = createJdbcClients(bc.jdbcClientConfigsBC.value)

      iter.map{ ast =>

        try {
          processRecord(ast, bc.itemsBC.value, bc.initialScratchPadBC.value, bc.tableCachesBC.value, jdbcClients)
        }
        catch {
          case e: Throwable => {

            // Save the stack trace in the scratchpad
            val scratchPad = bc.initialScratchPadBC.value

            val message = "Unhandled Exception:  " + e.getMessage() +
              e.getStackTrace().mkString("\n    ", "\n    ", "")

            println("Error:  "+message)

            scratchPad.setResult("unhandled-exception", message)

            // Run the exception action list
            bc.exceptionHandlingActionsBC.value.perform(
              ast, 
              Map.empty[String, String], 
              ActionContext(
                bc.tableCachesBC.value, 
                scratchPad=bc.initialScratchPadBC.value
              )
            ).enrichedUpdates
          }
        }
      }

      // (filter out any of these empty maps)
    }.filter{ m => m != Map.empty[String, String] }

    enrichedRDD
  }

  /** Process one JSON record.  Called from processJsonRDD and tests; not meant 
    * for outside consumption.
    */
  protected [turbocow] 
  def processRecord(
    record: JValue,
    configItems: List[Item],
    initialScratchPad: ScratchPad = new ScratchPad,
    tableCaches: Map[String, TableCache] = Map.empty[String, TableCache],
    jdbcClients: Map[String, Statement] = Map.empty[String, Statement]): 
    Map[String, String] = {

    val actionContext = ActionContext(tableCaches, scratchPad = initialScratchPad, jdbcClients = jdbcClients)

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

    // For every field in the input, make sure it has a value in enriched.
    // If not, copy it over.
    val inputMap: Map[String, Any] = record.values match { case m: Map[String, Any] => m }
    inputMap.foreach{ case (iKey, iVal) => 
      if ( enrichedMap.get(iKey).isEmpty ) {
        val stringVal = iVal match {
          case null => null
          case a: Any => a.toString
        }
        enrichedMap = enrichedMap + (iKey-> stringVal)
      }
    }

    // (For now, just return the enriched data)
    enrichedMap
  }

  /** Create JDBC clients, well Statement instances
    * 
    */
  def createJdbcClients(jdbcClientConfigs: Seq[JdbcClientConfig]): 
    Map[String, Statement] = {

    //println("jcc = "+jdbcClientConfigs)
    val jdbcMap = jdbcClientConfigs.map{ jdbcConfig => 

      val driver = "org.apache.hive.jdbc.HiveDriver"
      val uri = jdbcConfig.connectionUri

      var connection: Option[Connection] = None
      val statement = try {

        // make the connection
        Class.forName(driver)
        val connection:Connection = DriverManager.getConnection(uri)

        // create the statement, and run the select query
        connection.createStatement()
      } 
      catch {
        case e:Throwable => { 
          println("Unable to create JDBC connection to: "+uri)
          connection.foreach{ _.close() }
          throw e 
        }
      } 

      (jdbcConfig.name, statement)
    }.toMap

    // check for duplicate keys
    if (jdbcMap.size != jdbcClientConfigs.size) throw new Exception("Names for JDBC clients must be unique!")

    jdbcMap
  }
}

