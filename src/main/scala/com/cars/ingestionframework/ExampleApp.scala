package com.cars.ingestionframework.exampleapp

import java.io.Serializable
import java.lang.{Boolean, Double, Long}
import java.util
import java.util.Map.Entry
import java.util.concurrent.TimeUnit

import com.cars.ingestionframework._
import com.cars.ingestionframework.actions._

import scala.collection.immutable.HashMap
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


// Example spark application that handles ingestion of impression data
object ExampleApp {

  /** Check an assumption and throw if false
    */
  def checkFunction(boolCheck: Boolean, errorMessage: String = "<no message>", throwOnFail: Boolean = true ): Boolean = {
    val msg = "ERROR - Check failed:  "+errorMessage
    if( ! boolCheck ) {
      if (throwOnFail) throw new RuntimeException(msg)
      else println(msg)
    }
    true
  }
  def checkEqualFunction(left: String, right: String, errorMessage: String = "<no message>", throwOnFail: Boolean = true ): Boolean = {
    val msg = s"ERROR - Check failed:  left($left) not equal to right($right):  $errorMessage"
    if( left != right )  {
      if (throwOnFail) throw new RuntimeException(msg)
      else println(msg)
    }
    true
  }

  def checkThrow(boolCheck: Boolean, errorMessage: String = "<no message>" ) = 
    checkFunction(boolCheck, errorMessage, throwOnFail = true)

  def checkEqualThrow(left: String, right: String, errorMessage: String = "<no message>" ) = 
    checkEqualFunction(left, right, errorMessage, throwOnFail = true)

  def checkPrint(boolCheck: Boolean, errorMessage: String = "<no message>" ) =
    checkFunction(boolCheck, errorMessage, throwOnFail = false)

  def checkEqualPrint(left: String, right: String, errorMessage: String = "<no message>" ) = 
    checkEqualFunction(left, right, errorMessage, throwOnFail = false)

  /** Scan a list of SourceActions and return a map of dbAndTable to list of 
    * all Lookup actions on that table.
    * 
    * @param  sourceActions to search through for Lookup actions
    * @return map of dbAndTable name to list of Lookup actions that utilize that 
    *                table
    */
  def getAllLookupActions(sourceActions: List[SourceAction]): 
    Map[String, List[Lookup]] = {
  
    val tableLookup = sourceActions.flatMap{ sa => 
    
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

    tableLookup
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
    allLookups.map{ case(tableAndName, lookupList) =>

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
  }

  /** Run the enrichment process
    *
    * @param sc
    * @param config
    * @param inputDir should be local file or HDFS Directory
    * @param tableCaches the table caches, if any  (defaults to None)
    *        (key is the "database.table" name)
    * @param hiveContext
    * @param actionFactory
    */
  def enrich(
    sc: SparkContext,
    config: String,
    inputDir: String,
    hiveContext : Option[HiveContext] = None,
    actionFactory: ActionFactory = new ActionFactory ):
    RDD[Map[String, String]]= {

    // Parse the config.  Creates a list of SourceActions.
    val actions: List[SourceAction] = actionFactory.createSourceActions(config)

    // Cache all the tables as specified in the actions.
    val tableCaches: Map[String, TableCache] = cacheTables(actions, hiveContext)

    // Create database clients to use.
    //val dbClients: Map[String, Some[Any] ] = DBHelper.createDBClients(actions)

    // Get the input file
    val inputJsonRDD = sc.textFile(inputDir)

    // parse the input json data
    val flattenedImpressionsRDD = inputJsonRDD.map( jsonString => {
      // use default formats for parsing
      implicit val jsonFormats = org.json4s.DefaultFormats
      val ast = parse(jsonString)
      // 'flatten' the json so activityMap & metaData's members are together at the
      // same level:
      (ast \ "md") merge (ast \ "activityMap")
    })

    // Create the ActionContext and broadcast it.
    val actionContext = sc.broadcast(ActionContext(tableCaches))

    // for every impression, perform all actions from config file.
    flattenedImpressionsRDD.map{ ast =>

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
          println("PPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPerform")
          val mapAddition = sourceAction.get.perform(sourceAction.get.source, ast, enrichedMap, actionContext.value)
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

      // (For now, just return the enriched data)
      enrichedMap
    }
  }

  /** run this.  this is only really for manually testing.  See ExampleAppSpec
    * for detailed integration tests.
    *
    */
  def main(args: Array[String]) = {

    // parse arguments:
    if(args.size < 4) throw new Exception("Must specify 4 arguments: inputFilePath, configFilePath, avroSchemaHDFSPath and EnrichedHDFSOutputDir")
    val inputFilePath = args(0)
    val configFilePath = args(1)
    val avroSchemaHDFSPath = args(2)
    val enrichedOutputHDFS = args(3)

    // initialise spark context
    val conf = new SparkConf().setAppName("ExampleApp")
    val sc = new SparkContext(conf)
    val schema = getAvroSchema(avroSchemaHDFSPath,sc)

    val structTypeSchema = StructType(schema(0).map(column => StructField(column, StringType, true))) // Parse AvroSchema as Instance of StructType

    try {

      val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

      val config = sc.textFile(configFilePath).collect().mkString("")

      println("===========================================================")
      println("===========================================================")
      println("===========================================================")
      println("config json = "+pretty(render(parse(config))))
      println("===========================================================")

      //SELECT ods_affiliate_id, affiliate_id from affiliate
      // gives: 
      // ods aff id(int)  aff id (str)
      // 9301129          9301129O
      // 7145092          7145092O   
      // 8944533          8944533O
      // note:
      // `affiliate_id` varchar(11), 
      // `ods_affiliate_id` int, 

      val enrichedRDD: RDD[Map[String, String]] = enrich(
        sc,
        config,
        inputDir = inputFilePath,
        hiveContext = Option(hiveContext),
        actionFactory = new ActionFactory(new ExampleCustomActionCreator))

      // todo check that enrichedRDD has same 'schema' as avro schema

      // TODO Add generic output function.  Add wrapper and provide sensible defaults.

      //Loop through enriched record fields
      val rowRDD = enrichedRDD.map( i =>
        //convert all the fields' values to a sequence
         Row.fromSeq(i.values.toSeq)
      )

      //create a dataframe of RDD[row] and Avro schema
      val sqlContext = new SQLContext(sc)
      val dataFrame = sqlContext.createDataFrame(rowRDD, structTypeSchema)

      dataFrame.write.format("com.databricks.spark.avro").save(enrichedOutputHDFS)
    }
    finally {
      // terminate spark context
      sc.stop()
    }
  }

  /**Process AvroScehema from HDFS
 *
    * @param hdfsPath
    * @param sc SparkContext
    */
  def getAvroSchema(hdfsPath : String, sc: SparkContext): Array[Array[String]] = {

    val jsonRDD = sc.textFile(hdfsPath)
    val oneLineAvroSchema = jsonRDD.collect().mkString("")
    val lineRDD = sc.parallelize(List(oneLineAvroSchema))
    val parseJsonRDD = lineRDD.map { record =>
      implicit val jsonFormats = org.json4s.DefaultFormats
      parse(record)
    }
    val fieldsList = parseJsonRDD.collect().map(eachline => {

      implicit val formats = org.json4s.DefaultFormats

      //collect fields array from avro schema
      val fieldsArray = (eachline \ "fields").children

      //make array from all field names from avro schema
      val b = fieldsArray.map(eachChild =>
        (eachChild \ "name").extract[String])
      b.toArray
    })
    fieldsList
  }

}
