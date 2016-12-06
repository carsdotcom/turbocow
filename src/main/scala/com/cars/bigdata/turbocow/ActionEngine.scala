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

    //.filter { ast => // TODO REMOVE 
    //  val uid = JsonUtil.extractOptionString(ast \ "ALSUID")
    //  (uid.isDefined && testSetComplete.contains(uid.get))
    //}

    // for every impression, perform all actions from config file.
    val enrichedRDD = flattenedImpressionsRDD.mapPartitions{ iter =>

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

            // TODO log somewhere
            println("EEEEEEEEEEEEEEEEEEEEEEE Error:  "+message)

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

  // TODO REMOVE
  val testSetBefore = Set(
    "48c997e6-2461-4fab-b2b9-879d23dc73a9",
    "66844ab4-3e9f-4032-8f9c-2d82a05ebdc4",
    "2b1d98b2-67b6-4bf1-9008-7a0806370db5",
    "172e93ba-01b3-4bc8-b498-187c6f2345cb",
    "26715453-0a11-4a92-a19e-47124d51fb98",
    "06ad8a35-1c30-4b05-9d5e-5bdd956f62ab",
    "a392d648-cb22-4dae-984b-ddb09ec96032",
    "5031e97a-fb05-4f81-8f16-cad7839d561a",
    "f401f74c-1d45-4efa-a3f6-b73f9f13b166",
    "fd70c172-ebec-4150-a94c-e3557161c96c",
    "7c1ad16c-0a5a-48f3-a661-deb5c86c885e",
    "cf54a7c5-d880-4c94-a766-5e6a245dce04",
    "0e47bfae-da9e-4bb5-a405-3f17a5ac7b9b",
    "9662ff0f-273f-42f8-b668-49609090237b",
    "fbc118be-9ecb-4cd2-a577-4d4e803aeb62",
    "1f563241-f6e9-4bd4-b487-a71a332de1c5",
    "51d3a510-6747-4ab7-9e38-ed93f2b8ab29",
    "4e08501b-b498-4951-b431-7acb31f43792",
    "cc30fdce-3515-4a0d-98d6-854d9f6c3d47",
    "1771d9d2-7ebe-4d59-b029-021b4200606d",
    "6288c63c-bee7-4f9c-a513-9e1d3844a489",
    "87af819b-8b6f-46d1-847e-92d06cc0ebf4",
    "7cf59d5f-6ccb-4bfd-aff9-357d1e2bcb8b",
    "ab49c662-d41d-4cf1-89c7-c840640c32ed",
    "bc600e27-e603-4137-9804-983e09cd404f",
    "f0e6a8e5-e53f-484e-a2fa-9a6a2fb4aef7",
    "707aa36d-12de-4b07-99c0-e6ac3d21fd8d",
    "271cb11a-edb2-4476-bbba-1c5c68774fe3",
    "798850b1-812f-492e-a032-944d46ecd04c",
    "b94b5120-7e5f-492f-944a-c1ccffa4d20f",
    "118dd8fb-6a2c-458b-b9a3-b4ae04fec4ff",
    "4b618de6-9543-4e42-8390-06151c44b82f",
    "989ba5a2-a4ce-445c-98ae-b76015a62948",
    "832abf45-f9d0-46d2-a86c-640584afaded",
    "a135a1b3-dcab-4f8e-8581-ef3f9c8c1405",
    "6a91c6ba-1909-4380-a308-b980db6095f1",
    "1e123f3e-c20d-4b8b-8f42-26677d37ed86",
    "a57a0e16-d1a7-49ac-a1e1-2100b1a59b67",
    "68c93943-5d29-40db-b5fb-474bf1791e8e",
    "bad94f6e-b05a-4890-b8f6-8c29f849a6b3",
    "ede507de-3f22-4b49-9e77-d1062845b98f",
    "f1da0177-6dd3-4a27-8078-4f6b323e8c44",
    "7d946f05-95cf-44ae-9839-3079a1761f3e",
    "58aecb66-152a-4744-b59d-baf8811ccca4",
    "81a477e2-769b-4b67-814f-b7bd191a637a",
    "7d5acc58-0b2c-4ce2-baf8-17c9677ed98a",
    "262d8be9-490d-4e31-8eea-0f39702f6261",
    "e4f6f809-233f-4591-8b86-c2805857a34e",
    "8d4e5841-31d2-49e3-9c10-b8e389d07f74",
    "f96f7261-ca40-4949-b44e-3e42a466c10d",
    "9fc1eaf2-304e-4026-855d-50165d92ead7",
    "ef97cb71-95e8-4578-afe7-1a26246da0b2",
    "7a638067-ad96-4b70-bc03-07b9b270c637",
    "fe88e220-62ee-42b7-8989-503a0cace3b6",
    "c188e9e4-7701-48c3-92b7-18676537dfed",
    "daa47d06-c1c5-49ff-9903-81cc2b23124a",
    "3ccf0fc3-f049-400c-8d44-c51ef64799a6",
    "a8b535ee-621b-400c-b1b9-cbc03e24d5d2",
    "d569acd7-1962-4d79-b85b-6c575a441613",
    "ac7048e2-bcbd-4df3-ab19-996cf1f7b5a4",
    "13d99f74-382e-4842-b278-cd52d74ef741",
    "a29797a0-0d34-4626-a879-7722a6e31b85",
    "e7d7595c-6ed2-4d59-adfa-72904cd04bd1",
    "c0b56073-d8c1-4c82-bd5c-c3939aa3a051",
    "0d0a9c8d-8660-4159-bb2e-b892052c3a27",
    "35ffc12a-d675-45fa-8a32-438f865f7a9b",
    "b186ecea-6973-4466-89f2-e46c62859443",
    "9d7af7c1-9635-4e40-9771-90861f1a4d34",
    "072d296b-b2e7-4510-b4a0-c26618085389",
    "88f8f5d0-68f7-4ff3-8097-8b78d7ad500d",
    "e8e6f176-f06b-4c5a-815e-54c9e1589a76",
    "aa9f01d0-5297-44fd-a416-6d13f07722b4",
    "07a11487-c7fb-4e88-97da-e8453df839f0",
    "c7c6dda6-3ec6-4d25-a75f-ac49d15ffb2e",
    "2b9af323-2179-48a4-8bb2-6b825b493052",
    "50d497e4-d7b4-492d-ad82-393e5983a45a",
    "0aa43619-04de-4b53-b501-380fc1849c61",
    "e1d07406-e861-46e3-a876-be9905789e0c",
    "d6a85a0e-5064-4a6c-b623-5b890caacdae",
    "6576a60d-228e-4281-88bf-561621b4994f",
    "b77132cf-eb7d-4bca-8d65-b9e79480e633",
    "970f7a66-b0be-485d-b47d-70c0d87a6cef",
    "06e3886a-5155-4e31-bb22-b20377b8673f",
    "17fcc4c9-c9e2-4791-978f-a51361ed3da5",
    "bd3af624-82e3-4686-9810-2020a6da6b68",
    "5489e49a-11f1-4bbb-8477-2bd82953e66d",
    "8892930c-b3b8-4a31-8f55-bd020181e87d",
    "714985ce-be41-42f0-8214-cdc523525f33",
    "44d00534-e2bc-413a-9bbe-1427a38a922c",
    "de88e894-194d-49fe-ac4a-42c7c265fcc9",
    "84b32573-1634-4b2c-8084-2f6c14ba240f",
    "900f688f-30c1-4f03-8c21-f33fe735acc0",
    "a7a994b8-88a9-4458-aaf7-9065652da2bb",
    "35c57815-8c24-43f3-b791-b1ab492f4a66",
    "5442c6c6-cded-412f-9efc-dda032b9519f",
    "7475cab5-2ee3-45d4-8cbe-ab3091694e02",
    "79dd0fe0-1651-4163-9b6f-0d6bf7d35b82",
    "c4532fc9-7a65-4156-b244-5290b94684ca",
    "4e0bfacc-80eb-473c-8554-2fa52c8653fa",
    "9c099f44-bc46-4a86-a6f3-30c4b96b3e7a",
    "67c52cc0-d826-4905-8823-fad38ab71038",
    "a99c996f-f450-4a84-a67c-041960eea40b",
    "31fe8e1d-2a56-476c-a72c-a29945e8ce8c",
    "057c6f38-d09c-48fa-8978-72c3a2dd0343",
    "ff0d8c91-bc39-42d7-8fd0-7e9ffd628800",
    "7250925d-724d-49c6-a4a0-74bb232d1604",
    "2e9769de-fc35-4595-a02d-c47a7211ebb3",
    "4eb963e7-b33d-4cf4-a904-2ff7634185bc",
    "0b0a201c-e957-4408-984d-a2af64539af4",
    "50ad6603-43f7-4260-89a7-399ad9773bb7",
    "c1decceb-1063-42d0-baef-16b48383a322",
    "cc03fdf1-a9c2-4893-8c2e-bc6b4159be9c",
    "1c2df04d-9929-4549-8e6a-e1b6405abf62",
    "c5318771-309d-4629-a350-e62d8476d39d",
    "88cf4f71-b823-4c4f-8272-13ec8626b307",
    "105ce427-c716-4bc2-a5ea-291539eb8892",
    "c44ac89e-34b9-4238-88c2-10bd3766ce37",
    "4ce5e3f0-dc36-4404-9655-129616fb3eb6",
    "bb736194-7c5a-4a49-9d80-3c1e14bb14d8",
    "74d8b6c0-6a70-4135-8d39-2814dc3feedd",
    "c514c157-c41c-401a-b0c0-e5741d893f71",
    "d581ef34-0e1c-4172-a184-430371ed6b8a",
    "30936808-c6e8-4c9b-b070-5c7e60e29257",
    "c544f58b-e743-436f-a2d9-7427ad3f7a32",
    "e24f26fd-5b83-4435-9df1-8059be837806",
    "e66a2770-89ee-4d8a-96b1-11339e110843",
    "264976e5-745f-4208-979f-2df11b24dd72",
    "f8278702-7ce7-4168-bd60-7e565e088fd6",
    "a2f0f4ee-cfc2-44fd-ae1a-868d62d73c69",
    "b8c4668b-9156-431a-9967-0b66874f52e7",
    "90062d16-ce1d-48d8-b2c8-c2e4de2ccbc5",
    "ba6a03f9-29fb-4390-9376-ced5c4c08e92",
    "4968ad43-6878-4f01-89ee-a356db211092",
    "4b381f1a-d3df-448a-9cd1-791f1802d8e6",
    "49edfc6e-3cfb-4f91-b2e7-5776381a9cd6",
    "e254d4e4-fb29-4633-b438-e9de03f1f365",
    "e04fe3e6-92ac-47d7-a9a3-166680ee37f3",
    "3c3ff1f9-99eb-4d6e-ae29-36e001926a00",
    "46c29ec4-c97f-4de4-af62-69ae6d2307e4",
    "536c2796-3a80-4809-a8e7-a0a8015ceb1a",
    "6a0eed7d-8412-4634-ad96-e5a63c650b57",
    "3688ea4d-7d5b-473c-8130-deb41816b148",
    "539d404d-ed97-482e-a544-54e54aef64ac",
    "1acb0909-47ca-4f46-9ede-c548a3f1dcc0",
    "b2c1e93b-6de6-48e5-bc42-80616150a332",
    "dc16f579-9e1e-409a-9225-869fad8ff615",
    "f84f0020-290f-454d-b96a-f0447d6fd977",
    "e98d4fe6-ea33-4f6c-81ff-4f12ad55aba6",
    "ae548bdd-e605-4a43-b2b1-0a5e0e99f7c7",
    "5d0ec8e6-dde7-45f1-91eb-36e1e0466e91",
    "f8b0a841-7f00-4ea0-a5c3-294cb93d303f",
    "3cbf8397-8eeb-40cc-ab42-20773ad89318",
    "b25609ac-65f9-45ae-a35b-ecf34be58abe",
    "913e96a7-fbc1-41dc-9358-b208258fe45b",
    "1eff2795-d7a9-4de3-88cd-19a9d88b8aa3",
    "099bdae5-2d14-42f2-a862-e7664614b278",
    "67dca543-5d56-4f95-a01e-ed9a82b69fc1",
    "6ee1f030-31b2-44e6-b465-ed9e636ced16",
    "056aab8d-54b9-437c-8d32-1aa543387db5",
    "75ba250a-fe4b-498d-bd43-3b86f5544a90",
    "cc691a78-ca4b-4465-a1e9-e1882bba95a3",
    "65addc7f-b24c-4791-8dcb-8eec503688d3",
    "3f70dd11-34cf-4510-97f0-ee2a9cf12bab",
    "0d94336f-7dcd-419e-a943-6a7f3c5c4dc7",
    "3145f52a-aa5c-4024-b9cf-6df672a599bf",
    "90605b82-d995-41d2-94ea-a6986e8f718d",
    "2c409e2c-50b2-4cbb-ac15-9debc302a118",
    "1fc3cbc7-eb47-45b0-a533-cc4b0505d192",
    "26496224-b30e-4f26-9e34-4942f12f708d",
    "bdc6f3ff-d258-4c01-ad5a-8c8d0e7bb1a2",
    "4f0376c3-333b-4d80-ab69-85d472f8caa6",
    "ede76121-23cb-4c88-aed2-3c96de1b89a1",
    "9f1f538e-9702-4ae6-9f6c-35ef81df29ef",
    "dcc599a1-f0ca-4231-ab66-cae232bc5331",
    "08368c1b-11d6-44b1-8197-603624a69719",
    "4fc7e85e-a850-4445-b3b4-beef69491160",
    "4ccacae2-dd22-4ede-bad7-63fa47a3c976",
    "0b7f0b32-fe04-426f-b713-72a7dd196779",
    "43aec9cf-00e2-4145-bad6-b903250d63f7",
    "bf46acf9-6b95-4695-90d4-13c0ff6a71d9",
    "21ac0655-3b42-426a-8e86-0a89fdb896d9",
    "316dabf4-8fba-4b50-9fad-72f44e6aa3b5",
    "4d5cdbf2-9108-4fb4-9f31-a9bce71e006e",
    "fcd8ef9b-7590-40f2-9c1d-c5601b7496c3",
    "8a78071a-5e5d-49a7-8f73-38c2e5bf2c9c",
    "36da97ac-67be-4d05-abca-0ba9bcc245ae",
    "1b6736a6-3189-4164-bf2f-0badbcbc4fbf",
    "a138df71-6f50-4ce1-bfb4-16de18e621f7",
    "d52c776e-73cb-4b4d-b7e2-74cd87d15202",
    "adbcbf1b-6e14-4947-a9c4-1cf8b260f37e",
    "87c5e976-4c56-418f-9ca0-0f535eb80557",
    "daece226-1f92-4fab-bccb-30a3b5b05ef5",
    "969b193d-64a2-407c-8265-cb4b470900a6",
    "f698f321-9973-4734-917d-effa4328645b",
    "3c824d04-2515-47d4-b2a1-ed98a8833fef",
    "f5a045b7-d11f-4b29-a8a8-fec287c944e4",
    "3bcf3b9c-4ae9-48e7-a152-d66ad25a92b3",
    "40c32718-3b9e-4a93-9e56-0f8eff12429e",
    "a8774219-b833-46b3-bcb3-54c1525b7c84",
    "22aff728-bacf-4f74-bdd9-287cbf865858",
    "0b83b727-3ece-4149-bb86-41fb8814b9ea",
    "a1a52163-42d0-46c4-a267-935ec025e82c",
    "0d5c7f17-1896-4934-9f5c-c3ca4a3dab5b",
    "3bfae64e-7740-4646-8006-aeef92bc3b5c",
    "ec9e48d7-17f2-4043-8d2e-9788c19457d1",
    "f500d140-c8f5-4525-9621-df12652f1dd0",
    "0f95b17f-1783-446d-ac02-1c4c9dd7593d",
    "b74abc2d-de10-4d53-b255-27753f43fdb5",
    "b789c34f-6b56-4006-a7f3-97fa9d2949c1",
    "a1bd931c-c921-4036-a2d8-fac8e53ada8c",
    "daf91bd7-cd10-4c6a-8d1c-2775e0c4d517",
    "e4af8b74-1148-4b50-b294-cf719f1a49d5",
    "87d039f2-1b5a-4311-b834-ab76c28fb3bd",
    "9dad7287-493c-4113-ac5d-3ec614bca8dd",
    "0f726429-87fc-4344-9381-d27e395e1579",
    "227e3c94-8621-4ed4-af92-935af7cb27a2",
    "cdf0b37d-72df-4266-8f8f-df7250e1ddf6",
    "a796f98a-8f52-4d83-8344-05d2648817a0",
    "d84f10d4-dc2e-48a1-8d5f-096f074ff83a",
    "bd519f5e-3299-422a-9014-a964e8df80be",
    "76203519-d5f1-4452-841b-c494c99b69ba",
    "732de9b0-1326-4542-90f0-98f5adf79876",
    "0c2213e4-bfb6-41d3-8027-15a3a9a5e312",
    "0f1165d1-4a5f-44fd-bfc4-f5a00f55ebeb",
    "c0d1a43f-db10-4144-aa41-8de2a245c157",
    "739d8c67-0e22-4617-a971-fc59d8ebb543",
    "939d7b6f-e928-4767-bfcb-e04c09d0fb99",
    "f7e8a9a9-1177-424c-a383-35b374df3bbe",
    "6489d22b-bf89-46ce-91d7-5fecc7943e28",
    "5a277c24-85c9-4f36-be8b-d324d0690736",
    "50cb06ec-f808-40d8-b61e-966887bc5424",
    "5a1b6700-5d0f-4e6d-81fc-1fb70bf0a52b",
    "39803375-c60c-4190-87fe-b470660c544e",
    "87131e37-a4ca-4aca-9f68-694cebadf68d",
    "8541bf50-c338-4995-8d7e-76375916b724",
    "768d9fb0-a1c0-4cfe-a4d2-4161204f9fe4",
    "f758ecd7-aca0-4d71-8e30-38e064f17e69",
    "b48747dd-026e-46cc-8e33-3a507e4b02f4",
    "81a1e4e9-f697-4355-b2e4-ef2baebad31e",
    "3d34b764-e39f-413c-9ded-633ef2414157",
    "7c69c00b-224e-4b1c-aa28-a00104c88b36",
    "693d0408-3a20-4409-9020-67a766e0e487",
    "f19db7b2-f36b-424f-a915-ce3e882e8621",
    "6cefa20d-5aee-4c07-bc01-42a044142197",
    "a0e3cd04-d9b5-4575-bd41-f86d4d01548f",
    "ca968565-c90c-4401-adf4-3aa4cb7df413",
    "3b3f5b95-e296-4c3a-8d99-0d0734f6fb1a",
    "dede7610-2469-4ac4-a476-3365958e8a6d",
    "10b8d831-5e85-4998-9fbf-8fb8580083fd",
    "1ee413d0-d82b-4efd-8e58-a7505f8dca50",
    "edfe88e6-9d93-4562-9f80-d6e942f29628",
    "4071dfee-0696-43ff-b578-6c7d0d2d0b2d",
    "62c1879b-f595-4cef-9cdb-e5200a150831",
    "076186a1-938c-4d04-aec9-94b91d9809a5",
    "c461cf4a-4ba6-47ba-ab5b-213e4edca7bd",
    "a3e6b276-2861-4e0e-9ad1-b782ae1f955b",
    "4ae22a9f-abc8-40ad-9425-24f391781f15",
    "725a0a28-a2cb-4470-9c72-95affd67a983",
    "9c331a19-b1be-46ce-a551-6a36d2e0f0e7",
    "3d765139-13cc-407a-ad64-35089156def5",
    "529d2452-9942-46c4-be8a-b9a192fddda2",
    "a3a2cbc7-b4d8-4590-bc00-c242102212d9",
    "512875ca-c685-4678-85b8-8922be7d265c",
    "276d1aba-5f74-4717-b515-05a612e0ed05",
    "f198e886-4d99-4db6-883a-ee49a523ed2b",
    "e7c41b32-5985-4830-ae18-29f8bdf8c6cf",
    "ffcf767e-a6f0-46da-851c-557498782953",
    "66bc1a12-7174-4d4c-a619-83a85065514f",
    "7b2fc0e9-8651-4093-96a3-61d9c47d776a",
    "4a70eb06-0aab-433c-8278-b52cfbaf76c2",
    "15fad7aa-29ac-47ab-ac86-549cf318429f",
    "70afb107-62f3-44a6-bac1-8380044648b9",
    "6ad7f788-3d95-400f-97ab-2d0a67eab75a",
    "9c3e1478-be34-47ae-9a8a-3ca1994c06aa",
    "e408b6f7-351b-4ef1-93b3-721c3b4bd430",
    "198a4562-690b-434f-b1a7-877d1a9add9b",
    "4bdfb1c7-38d0-490a-82f8-9986b1979cb1",
    "262a00ae-2263-481c-ad1e-af1eba5c234d",
    "83794f6a-36d9-4cdd-a12f-e257ee833dec",
    "789c2d1a-9a93-4464-bdae-74912e19e09e",
    "574ef7a8-9c0f-4a3b-a7ee-32977c50c1d3",
    "03056c3b-9daa-4e62-bdbd-32749945394f",
    "23782cbb-342d-4999-9645-398efcc501fd",
    "87f91a86-4b84-49f5-97f0-cfc3efaec866",
    "725a010a-7c7f-4c23-80eb-0bdd6ed14e25",
    "bcb0ebe1-f2d5-4a0e-90db-480ffca95b00",
    "7935eaa0-5fbb-4c1d-a21f-183eba503b33",
    "585e3025-ddf3-47bc-a30c-e6f963ab2605",
    "d4ff3104-ed71-43c4-af8c-241b016bd67d",
    "73d021e3-566c-43f9-9b0b-f67636fb20fc",
    "4505a8ca-6693-45c7-9813-e313f8e563d1",
    "7070b4ff-faa9-4f6d-b125-12abd608817e",
    "cfa1b56d-7b13-4383-a13c-b59e9f1ca377",
    "6b84ad7f-76db-4d09-8be6-261cfe6f8bb3",
    "1503848b-4a02-4bf2-9773-10ed5ff571ac",
    "60b47c08-bf99-4e8e-8297-b1fbd0618261",
    "aedd6308-c473-4c93-8033-4baf3d1eb525",
    "2cee6ee8-d038-4adf-902d-ed724ca53650",
    "d68040da-d486-4cf7-96eb-5b1c52ccc1b8",
    "a9a23448-163d-4f05-b78f-769ae4c4fd9d",
    "371b847a-2309-41d5-a3f9-78cab12726c4",
    "48e3d846-f213-46de-acf7-89e4bc153525",
    "be707843-e30f-47eb-aaf7-c9dc1150fa39",
    "eef80ab5-2a65-41e2-9073-bee962e287e8",
    "370ee3f9-5d01-4d37-9bf9-98748f7a69f2",
    "38b2a127-fe05-4814-9dc3-bd1707597406",
    "86b2cf1b-cf18-4782-91f8-2e36f60ff0c0",
    "3578a97e-5ba8-408c-a2ed-7251a34a7ff5",
    "6c1c75dd-9bc0-4e0d-8358-2cbd02b58ce9",
    "1ff44cc6-4b2d-4821-a00a-e0fd1e03e916",
    "fc27c872-d7eb-4aa2-af12-8398b3540c24",
    "2dd6c21c-e0f2-453f-b8e5-13b5703fed26",
    "7222324b-cfe0-4333-b510-d092888450b6",
    "9cdf1ae6-4d5a-43b6-be6a-ed20621b64dc",
    "2e4a8dd1-2650-4c4a-8369-5f6f3c805c00",
    "5a37e492-29e1-4c3b-88ba-71f5fd4ad9f5",
    "92cd8e53-2c49-4e39-aa77-975d2ee69499",
    "4611b28c-78f3-4eb6-8f01-1b8b27e583af",
    "4d6d9b09-01ec-43ac-b8dc-101481df2059",
    "1441b7a5-8aa6-48bf-a7bd-213b4b77fa34",
    "b19dfe17-8e9e-4be5-8a85-14d8abbe39dd",
    "407704f4-dcb6-493c-8642-1ea2ea62e493",
    "d6464397-dfe0-4d5d-a98c-5a989de2ae10",
    "8d85b4fc-17f2-4ae8-87f3-2d5937130b16",
    "869bcd0d-7ae9-4fe8-9d9b-3d981953c920",
    "465f3004-f6b9-4c94-b9a9-4260481530a0",
    "03289a7f-4e1f-4a90-9989-993704c097fc",
    "5b815752-d769-4b09-94d7-3ea93d9f2d0b",
    "a2ab7b43-3f6a-473a-9a0d-e12a488eca14",
    "a685495c-a07d-47ed-840b-6a8ade22bf16",
    "b73217f5-1734-4491-9cab-7dfe79093108",
    "53a11973-f9b1-46dc-b0aa-bb0b6ea3e8da",
    "2cd2f517-879b-453d-83a9-0d66b174fee6",
    "6a514bc5-e874-46fe-ba01-46f345a38063",
    "11536c95-359a-42eb-bb7c-b9b2c87afc57",
    "9d708050-e83f-4ef6-b31f-4cb213a3c9b5",
    "5e419157-16cd-4675-92d4-74374b5809b4",
    "4e65d3f5-a465-4fa3-ab20-1d5d541ebe8a",
    "07c35b08-a0ef-4539-9361-4087950b0ac0",
    "3fe7a840-bdf6-401c-a9f0-3fe1db47a21b",
    "4890a53e-95d8-4775-93fe-d20b657ada23",
    "2328b070-c931-4366-8ef2-d98b2d5c6dd0",
    "ae132358-04c8-4721-8969-84e0c96ec70e",
    "0f1638b0-faf5-401b-9f61-2093ea77e38e",
    "c57d83de-0bfb-448f-80c2-ef880191fa73",
    "8f2497ec-8903-4b0c-8d8c-d2361e34edc3",
    "e13ed404-3903-40ef-9f25-9095bef6465b",
    "3cf92e55-6c34-45ae-8f31-34887461e9d4",
    "f5c7455f-30c1-4d3e-ba2c-904aa962e4cc",
    "e5dfd94f-73d9-47ce-9e12-510f51b00883",
    "493a32dc-6aee-4327-9099-aba9be63f2d0",
    "a6195447-43a6-47d4-bce9-4fba9b1b54a6",
    "19f8ce02-4a34-42a8-8695-fa99067dbdd4",
    "d04be1f3-644c-4a85-87d0-9e0c29640bbf",
    "b16e76e2-cd5e-46f8-ab30-4c135b39605a",
    "7215dfb1-f808-4efd-b4c8-18a1d905f765",
    "dce38d61-043f-4082-a9d4-5cc242e9834c",
    "9ea69214-eb06-45f6-9987-9ac87af91930",
    "cd01d133-1d63-4817-8a5f-5c854ac8e63e",
    "fbf3671e-5401-45d5-b7cd-ab773bdf308b",
    "fd18a8c1-f3a6-4743-b5c7-93e305e2f7a4",
    "e70b517a-62ce-49f9-8f70-017290abc71b",
    "82d0e575-7525-448b-b49a-25fb17255b92",
    "2210d892-6c93-4214-b3a1-cbd18ba1c8ed",
    "96954a25-4f8a-42b8-854e-a333484234ad",
    "ff7077a7-325e-4f55-b3f5-07884aa2abd4",
    "c89f507f-d31e-473a-a986-b13f253325f1",
    "5aae21ed-1366-40a3-b5b2-63ba67e0bf5c",
    "e9940bc2-7474-4fcb-810d-5b20bda2517b",
    "d73a7490-4fca-42b0-8c76-065d21474769",
    "f7dbeca2-04ce-400b-9529-e15a7ca70f7a",
    "25f3e14b-80db-490e-8797-4bd13df8100f",
    "c6228ce5-d27f-463b-adaf-e45fc2eda303",
    "b26a9a5e-4d75-43bb-949c-045cac6fa864",
    "7dbbc163-d2d1-47c2-af7c-60d59864e8c9",
    "6ce4fb25-e194-4540-b303-475665787e2a",
    "64902422-10d9-472c-9062-8b1bc0e27270",
    "d1d98746-e63c-4cf8-8444-311ecb3922cd",
    "14cfc8ea-82a7-4512-9c26-6a40193bdc0e",
    "4d3f19f0-7c24-4a7c-bb60-17dc52dc2835",
    "8321df6f-d914-4a12-a22d-0642dee408e8",
    "6854c408-7d06-4dad-bc7d-4719a1361973",
    "306a9891-2f93-4cb5-a85b-70772925ffdb",
    "181c15b2-d51c-498c-a20d-b2ffb82923a3",
    "4851f985-3288-432c-a56f-daad06e6ae60",
    "9ef7ecd3-1ecd-4ac7-b158-e083564dfa4a",
    "566ac098-b3b2-4930-b947-5a0bc5613e44",
    "30fdc499-eb8c-4f56-a781-8cce6b4d3ac1",
    "ff3ab2fd-3544-4d12-ad26-583347dcb2f7",
    "29b986d7-14a7-41b4-8e10-c2a39271ef01",
    "546ecfba-3198-4067-8a57-36deae6f55f0",
    "3b1bd36c-fe0e-4013-9ffd-2d024b8cc1b9",
    "3d8adcee-9468-4676-92d0-c2d7b78db472",
    "d4e663fd-0694-4de5-8ec9-46cdc7fa7ad4",
    "a49d2b5f-032d-4813-b65b-c5c707f27d35",
    "aabd721a-5e56-4748-8bb9-1ad74100f77b",
    "90581121-45f0-49a7-a621-7774456552d2",
    "0c00ce19-0357-43bb-9d25-98c94f43d8ef",
    "c4e50ecd-d7d3-40d7-b04f-458efa86086f",
    "c702b0d2-a58f-43da-b053-4b7fdd434f68",
    "101984cf-b0b1-44ae-aab8-36f37e5a1dcc",
    "9a333da5-6393-44d6-8e22-055e7d8d3075",
    "68fdef9f-ac20-402e-b91a-5ff02c756bd4",
    "4b8784ba-50f1-494c-837b-a28e42c1d293",
    "6b14d19a-2fe0-45ff-8ab3-adabf080f486",
    "96adf26c-0a34-4142-887f-5c896613b632",
    "eec56420-5bad-47c2-812c-ecd7e14a24a9",
    "976cb133-52d6-4a3f-bec4-18fcaab07d70",
    "0dfb41b9-31c6-4c44-b9bd-f026952cf5c5",
    "4f02dae2-11c6-4acd-a64c-e94a2b5152fb",
    "1d0a6f06-80e9-4d1d-a7bf-3aa781b28f2f",
    "76110f7e-f975-425c-b288-be861e911485",
    "769427a5-7ead-42a3-aa36-e0315a54fce1",
    "ed929773-4af4-4069-a2b9-916871d09aa7",
    "257885d2-341c-4fc0-8107-889f440cf269",
    "871e2db4-976b-435d-875e-db5b7c14be3c",
    "ed890c0d-b9da-424d-9a88-976138b603ba",
    "320b8ec1-86c7-4e1f-9746-9c34df32e86c",
    "5ae9db84-b262-4478-b776-1353bb0a1c5c",
    "fae69bfa-dd62-4ae7-8d5c-04d925520817",
    "ab2b3bdd-1887-4ed0-b9f2-5d90e48dc0f7",
    "fb958a48-01a0-4b01-800d-87acfaf023d1",
    "af4f618a-cee1-49ee-a13a-3442b967232b",
    "46274d35-a798-49f8-92c1-f7fdee980041",
    "974bffd2-f6f4-47c9-b83e-7bacf0b05955",
    "efd213a9-bc62-4025-a3f5-c43bffff39c1",
    "2cca2a9c-20f0-4c63-9aaf-23662fb62049",
    "0d2f0c66-1261-4683-87ca-d2ef210a9d91",
    "e808a082-f4c5-4e3d-92b1-76ccca119850",
    "92cb23ae-c0ba-485d-a686-47426b12eb77",
    "fee9fa41-6d2e-43ee-9870-4dd30745b26b",
    "8a2aac42-5fb6-4a50-93d3-f6ea701febcf",
    "af9ad8a1-8189-4ec2-be4f-d07d06721d15",
    "be33c73a-ed41-455c-b769-72036c6e6b09",
    "eea81472-9f14-4013-b663-1ffb1fbc3e76",
    "03fba7a6-04fc-4ff6-b5a2-6d57b98a6cd2",
    "7b1516ab-e3d4-427e-af20-94b33c20c459",
    "5cc868ca-3eb0-40fb-8033-78a8effebd99",
    "969eb0ff-128d-48bd-832c-680ccbd0d62a",
    "6ac6e70b-2e6d-43d9-a522-321e22b67a79",
    "bef8854c-30a2-400b-a8d9-70c80cfa8a2d",
    "eb9fc8ac-79f0-414e-8de5-3ee08de384b9",
    "ca850ddf-2a46-4df4-a1ab-00981ff16f24",
    "af7125dc-eef8-4fbd-be08-01f2e9efb5fa",
    "02402ee6-5ea0-433f-94c2-7810c069f48a",
    "817d7fa4-19fc-4747-a697-79d0972b2b2a",
    "a2645e87-8437-423b-ba88-6dbd1cccc4e8",
    "eac9d580-e796-4da8-a434-04b2f39a2e4c",
    "af0720b3-1885-4a8d-8fba-d87829079072",
    "590fec7b-202d-4916-aaa2-f5b290ad07da",
    "5521e903-3dd2-48d7-b508-50a72ade00e6",
    "f0d31c53-e9b2-41be-bc7b-6f9591795e2f",
    "0e252231-8bf9-4d0d-8b62-8e5a57cf8c59",
    "d1a90277-1a58-43bb-85be-d5f65a476f84",
    "63c032fb-f551-4109-93c0-96db1379b4cb",
    "16acca73-c723-45b3-b680-e4c7c003fe10",
    "e60a4d6c-10fd-46b4-a08e-e827670489b4",
    "d6d73714-b366-4e79-8aa1-79263c64c649",
    "d9d34e43-9900-44c2-af82-c41ddfe09c84",
    "9afc2397-9bc3-4d6e-b28f-e3df5ba612a9",
    "bb7edab3-c059-4019-a26d-0615e91906bb",
    "5c130a26-4820-44ff-a812-ea2b7c677826",
    "fa21f855-007e-45db-b84a-158f23cc6e67",
    "46fc5159-afee-43a3-9650-ef3f28e1ac0a",
    "b364bb67-f529-45d8-8b32-c0ca7a62ea0c",
    "626202b4-bc97-46fc-a0e5-e517b238d203",
    "6e6eddc5-3caa-4810-84c8-fab3edafb4fe",
    "c17fd715-b2ac-4c92-80b4-1be2aea7eaf5",
    "826a9574-16af-4217-9d3c-41dc4eececf2",
    "74cebc54-bf4c-4d63-bd22-9f76e725510a",
    "e3ac53f3-fb58-4415-94ee-c3d4ca4e645e",
    "99faf2f2-5a77-45d4-a029-755912f30a54",
    "66db6c22-9eb7-4c32-9957-83ee3823d4cf",
    "52805091-6116-46d8-9c30-5a15d410a015",
    "3d502cc0-5e5c-408d-900d-5e79b0c53088",
    "d1ec150e-cba2-4870-9e27-0d59b9498c0c",
    "eac6f637-cf70-44dc-9b54-c9c0825c294b",
    "c66b65ef-578a-479e-8493-c06990a68337",
    "5232274d-c14b-4b7b-aec2-370a9ff1891c",
    "f0805ea4-d9c0-4822-a2ec-d15133130315",
    "15a67805-c333-4f4b-a2a1-7816a42aa743",
    "a2b58c1e-e658-4450-a798-4ddac287697c",
    "98f439c2-77b2-4526-a620-61ebb1975f47",
    "6dd66120-7a7c-457c-a93e-b009bff5f96e",
    "7c0b0d93-b5c3-4ff7-aa04-b814bde7ba5f",
    "0daea1e9-0e71-473f-b549-22487132a565",
    "f2ea907a-e7a5-4779-beac-bdbe1ab6b2c7",
    "950b5ab1-5e0a-4585-b2b3-a9de85d1c53e",
    "36cd53a2-d6ab-419a-a6fc-9647c3ce8976",
    "7471f18d-785a-4a8d-b873-a68089c0e5b0",
    "c07fed3f-4945-4f73-adcd-329968da7636",
    "7763e006-e57e-428c-b201-105e34c7ea01",
    "048a1da6-2c49-4d4e-bd2b-b6cb86aa4850",
    "19ea095c-786a-4a4a-865e-2439a26f2229",
    "2ed9723b-1c45-4bda-936a-256982c702b4",
    "ac7a4c46-d8c8-4a33-b2a1-e173c3a05d9d",
    "5a877a96-e91a-4349-9fcc-8e8a3f85c297",
    "e357ce40-b3ec-460e-ad5b-018cf6323508",
    "e49b7e55-3ffb-4c34-8391-f0c03eef61b3",
    "ca412c8d-c008-4afe-81c3-4148e5a8e84e",
    "ac4afec0-a214-45eb-8797-ffe1ef502c02",
    "e1308621-29c0-4d42-9f93-a1a0c0340f71",
    "703b92ba-b74b-4f6e-b0de-52a3b8899106",
    "46ea5fe8-dc12-48a1-8908-5dcb83736997",
    "b3627057-d6ef-4f68-bbc0-2336e007e5e3",
    "d0a867ec-ddd4-4226-bf90-027f2cd14c48",
    "271bb0e1-a383-4fba-85fe-0109dd054e9c",
    "1f70cc20-518c-4a9a-8fc3-b8d401d391a8",
    "881d4ab8-1f4f-462d-8d23-a102b2f3d145",
    "dae05fb0-61ce-4b1c-8787-cab2235d539a",
    "3ef2ae6b-f9d2-45b3-b20e-6fd47464c680",
    "abbb1274-1a52-41f9-a500-5c1d1ab65fcb",
    "d420c7c0-8894-456e-bbb6-ebd473a18add",
    "d49a5f32-be05-4704-abf4-71831620e32c",
    "a279dd52-0e42-4fb5-866c-1c5e81120730",
    "2482758d-d6aa-469c-8d95-7ac10bf6472c",
    "8d788223-6bbf-427c-9157-5394e57cadaa",
    "6e97cf0a-50e4-43df-ae90-e5328c412a03",
    "b01a6029-3f32-44a8-8b74-647c003e2b09",
    "7a89d08d-6dd8-48f7-98f3-e8ca24f2f110",
    "c12bd49d-13a6-427e-bbaa-f08afdd16aca",
    "bf9a204e-2941-4067-bce8-c2d25a47e8a0",
    "4db3f077-b245-4539-a578-9aba8e88b542",
    "32b95def-2671-40f3-a40d-04c85f0b52d7",
    "f9fdafff-08fd-4d0d-9a3c-445147c36967",
    "1599e5d3-4778-4b70-ae22-817cb9107188",
    "f34f745b-ddf2-43c6-9c7a-8325c127b4e2",
    "5f6d0972-cb10-4de8-a945-71c9578d3689",
    "9b730ff4-f560-40d4-8e76-b6a8f997d143",
    "1ac22cb3-9919-44a2-9451-711bf7b6dcc6",
    "862ba571-7fbc-4bb8-885f-48e2d13f0d3d",
    "e9dedfbb-45dc-4f1f-89ff-d8edd4ffe674",
    "bc4328fb-5a10-49a4-aa36-f75b292b61cc",
    "144c78b2-ff56-4611-8932-abb5d5b9b0fe",
    "9ca89857-b0e3-4352-a1c6-ff63a842f186",
    "f7ba1ac8-83dd-4ee3-ab4d-d80122cd4e89",
    "63bb94f3-84f8-4595-bedc-1a2ae4b2126e",
    "42469dcf-f4a4-478f-9622-1da785cfb434",
    "d789b24e-871b-4587-8061-41ee1ae6117a",
    "199b0632-9a1e-49f3-af3a-cd21d03140c1",
    "7953d83e-717b-4559-9df3-3014f0a9db8e",
    "46bdf6ce-72ff-40f8-bda8-4f4caa97e793",
    "6e4c6b12-b075-4ba3-955a-6eaefba6408d",
    "35fa53bb-1d56-4880-b4c3-e3da21b87120",
    "34a2d993-e808-48ef-a398-fcfff624e663",
    "2a5e0ee8-93cf-450a-900e-d3676dcc35c9",
    "89169c04-0d82-4695-87c5-d811cd326108",
    "7f23c120-37eb-4c86-8f83-5b10f53087e2",
    "a78c8bfe-33b3-485e-b184-90ae9adee4ec",
    "edcac515-efa6-46dc-b738-7562c39741e4",
    "030c7233-6a1c-4fc1-9504-1ec02aced371",
    "30290077-1a63-41ce-9fc2-c6b435e1b351",
    "5c780a84-82ae-44d1-a40e-d37028f623a5",
    "2fcbda42-eeaa-484c-b77d-9e7d905b5f27",
    "ebd297d7-8fd6-4168-b34a-1cb129d88ca6",
    "a8c879d9-1599-4001-8956-e2d1def02bc7",
    "710bb5c6-9910-43d2-8ec3-5d0e7e9ca19e",
    "e62cbc6e-60f6-4fbe-b425-e3204d645f2a",
    "d8cabeb7-f39a-481e-8792-1509dee598c3",
    "6a364bf9-af21-4f0d-96a6-080797d1038b",
    "92f18736-b7b1-49bc-b732-e6e322481392",
    "f1257b66-78f8-41d8-8e68-8fa32fb984a5",
    "ff2b36e1-b900-43ed-9f3c-607a55f4b7fa",
    "2a6c9060-3401-480a-9f3a-72e82ac4d7ba",
    "911a38d6-62e2-4e3d-af07-30343fde70aa",
    "295099ca-d58a-49c4-a17a-7990eb143869",
    "dafa6bc6-39ea-444f-a4c5-6f649479c14b",
    "4cc08949-4e5c-4bb6-bbcd-7ea47e889a1a",
    "d947c37e-a8e5-4e2f-8012-9bb8368c871d",
    "efd83481-85f9-482c-b9f4-10f5f56e1091",
    "dc10cb98-ce85-4787-944a-81eaa8a0fc9c",
    "e42b230a-4b9b-48bd-9549-520baa1317d7",
    "adaad4cf-9d5b-4daf-97de-15a08c7c2e5b",
    "b27afe35-bbc9-4f7f-8d6d-a5df367ccaa5",
    "5c6ca4c5-5cec-46f6-9c50-a4c32a84e374",
    "7a3ca897-a5dd-4d68-a09a-d5b8004ddd4f",
    "11327ec4-dc92-4fdf-9876-d17a4fdc3b75",
    "23651d80-716e-473a-af17-1203d484aac5",
    "ffc3b139-6d6a-4a84-a25a-fc8630a31043",
    "eb692538-8cdf-40d9-9705-40e591171966",
    "80346d92-0019-4848-afaa-09a7c2a6b6a9",
    "3808e299-af57-464d-9d7d-84cf64c725ef",
    "855be8dd-13b8-478c-a82a-88ada8336e05",
    "14f693e0-5726-4f29-9b4d-bf37e2dc0304",
    "c1b05864-29cd-4c53-b342-9e5c550d050d",
    "8f3ebce1-6c1c-4a6b-a79e-2653979aca43",
    "750b2fff-c5bc-428b-a9e5-3fc3b418ff4a",
    "53749d9e-631c-435f-bac1-8a475464b134",
    "8f64e384-a191-4543-8709-da478768a9c1",
    "b06a0de1-a646-41e2-8508-b1097815c22c",
    "d106f334-0e57-47a2-8a0b-1d436eb18ff5",
    "d611be58-42c7-4e63-a80b-a4bfe60ab092",
    "20b7fac7-7b6d-4baf-81df-de3db5cf5a67",
    "2d7bbebf-0a83-4c6f-9936-f517682712e5",
    "bc1534bd-de79-4f89-bd85-cf61271c82a0",
    "c452e774-577e-4773-b1fc-0dde3edd8a25",
    "59ded97c-925a-45e5-bf04-dbe41f74d53c",
    "c22e196f-65b6-45dc-81fa-b337d90ce636",
    "d6f3389d-bbe9-48ff-8228-49149f37774c",
    "05759645-aa0f-4a14-b6b0-5f9943a5bbc5",
    "6aa8c128-8b95-4bbd-9277-ade54c7cce13",
    "7a23a6ce-fad3-459f-a5c7-df98f7e8e589",
    "c9e7995f-76bb-431c-b227-8fd9598b49cd",
    "abe705ed-b196-4778-8b72-2b293983abbf",
    "136fd21b-35d4-4f2d-8b91-49ffd747abc2",
    "0ce55984-5141-442b-ae8e-223b881d23d6",
    "6cfa3272-af0f-4bbd-8302-91ffc78522a8",
    "e3e2cafd-0393-4aa7-bc29-3e7eecabe4a9",
    "1275c1ec-f7db-4fd2-b127-11e6b9ef2b66",
    "01a4680a-ef84-4f91-a0f0-98bb4fd91bfa",
    "fbbe5923-1936-479c-bf57-625a8519d692",
    "695587f1-36b1-410c-927f-ff3aad3cc385",
    "ce054f52-0bc3-4a8d-a0ac-ed3056a9cb6c",
    "d4260dab-66df-4164-8c79-804e2779e9e8",
    "eb226fb2-1dc9-49b4-ae2f-a5724ec38844",
    "dd156452-b1bd-4e9a-9148-ca67e6ab493c",
    "6a888c49-8203-4bd1-a61a-7608ffec4f15",
    "6eaed309-334b-4674-99e0-82f6a6fe4d9c",
    "0ec4ab7e-87cc-41a9-816c-df141ee003da",
    "1b97c020-3459-4b90-91c3-a3fe16bf30b6",
    "f4a82d19-d770-4436-af00-eb800c90a06e",
    "aea3b549-f209-406e-81cf-6eae1bc431e0",
    "3e503b30-968b-43a1-adc3-94fd87233823",
    "dd61bf60-4d3a-4d83-beff-0a302aee2114",
    "f5df28b7-a1b7-481f-a337-ff49dfca8100",
    "030b9ba2-74e3-40bb-948a-a6f9cf5a6d09",
    "57df7c67-bb67-48c3-ac7a-ebdea0d85d6a",
    "2dbd3287-7e71-468c-87c2-7c092a3b5576",
    "0cb29d31-7378-4ad7-a4ae-aa2bda561cf0",
    "86a24c1e-cc9b-4ed0-9d60-30fef507994e",
    "7dc8bec0-e68d-4650-908f-3e572a5a6cdf",
    "0c64b38e-b727-45fe-a6fe-4200040e673c",
    "8942f61a-b65c-4746-b755-9cef08fd0771",
    "d47e6cd8-4125-49c4-8c88-3317df79b58b",
    "a8bf0a25-453c-4063-8b12-1239989e9536",
    "cc50b6b5-0e25-4436-a2f5-f377c6c8fc80",
    "8ae06118-875c-4a95-8f20-449516e01224",
    "2c8d0ef3-2a3f-4a1f-a988-c5cfb0ab90a9",
    "d6e94173-a861-43a5-965b-70b0a2d46ac9",
    "9f3f58d8-f296-472b-b3c1-38b4da17dcaa",
    "6a2f5ec5-e4d5-4229-b5ba-f122dc564d98",
    "549d755a-eeed-46a1-ad34-3d1f1a193724",
    "0a60a73b-b5ff-4ab5-8dca-2ac0a0553921",
    "0cf1d712-d1e3-4a5b-b14e-00473228e444",
    "69fff1ed-3dfc-41a8-8f0f-23bd8e6257dd",
    "a8ba8edd-4197-4aec-8caa-7777e6ee4c17",
    "58719421-6fac-4df3-afd0-12ae009601f7",
    "0b8e5a65-a38c-46c5-a0c3-25e8ca52e9f8",
    "3d89383c-3e99-48e0-b70f-3ebeb0cc1e2b",
    "f4945d5a-b743-4cd4-91d8-226376a27e5b",
    "0fa74a15-88f0-4a14-b2be-69a2b9459c62",
    "ff79f32e-b813-4dac-8789-546de58ac80a",
    "39bab5b1-e3e0-4f29-a64c-df07649de921",
    "4659eccf-ce79-4126-be1d-d88c42220b13",
    "1777e96f-cd6f-405c-be05-d0200bdd0a6e",
    "d6928d71-4593-47b8-b136-642e8171f714",
    "5425303b-a6ae-4c0e-859a-b896471084bf",
    "ea579ae3-3c39-42db-9e4a-affbcab485d8",
    "b5eff75e-fbac-4fc5-914b-429b9ebca81d",
    "9137e179-e1c0-4766-93db-ced88935f35e",
    "8bcd2401-3d39-428c-879a-fec817eb209d",
    "906216a6-0ae8-4a4e-9910-42e44a551b36",
    "b745d1c9-34a0-41fc-8db7-c3bfcee1fc7d",
    "c56520d2-f243-4cdf-9f66-525e63b75426",
    "4b459c84-650e-4f62-8590-ba53db922d33",
    "6c3755c1-5dfd-48b4-a5d5-9562dc4834ff",
    "4a06c40a-a89f-4e2a-8c8a-18fb6edd3dc5",
    "4e47cc0c-e3b0-4212-892a-056292fd00ed",
    "e9ef1ef2-da7d-42b1-897d-cbb9b1bf2bfa",
    "8a60a072-bb07-46eb-a6a1-a717d362bc4e",
    "40e26104-2245-40dc-ba70-12d40452e940",
    "615da889-cfc6-4289-b458-3cca798d8fad",
    "960c7c4f-fbd9-42a7-938e-6dfc90f73539",
    "d76b9ac6-b40c-4d2a-990b-06c4cf8f3bba",
    "0a464a72-17b9-45c8-b7c2-96e948ad6f10",
    "7e74d71d-fa3e-4c6c-bcef-b220d5642919",
    "400e44c8-5868-4a02-92ee-8af83c05434a",
    "3b7f153b-45d7-4433-9c63-9ca9180ccb02",
    "3980f312-456d-4b93-a3aa-f4ab82c70c50",
    "5521c69b-86a7-4dfa-81f8-4795e91f817c",
    "dbef13af-83e9-45ed-8c75-182bcd7e65ac",
    "5644025e-0a63-4d90-8ed6-1c91d753f9f0",
    "4a68ecd2-0ffe-4086-a4b1-a3babbcfd55e",
    "1e230bd9-1697-4ea7-bc96-e583da6bdf80",
    "3fd7860c-7530-4dc8-83d0-91e8bf30dfe5",
    "a031e240-f18a-4ffd-8819-2e98e7d90eb2",
    "28c6691e-8699-4fca-8e6c-a9cee6ea146f",
    "741aef44-791b-420a-9934-2656b2f5a0fe",
    "6ef0eccf-383b-4ae4-9623-b11a206d8108",
    "d1e29763-eb4c-4cf6-ad2f-32e272624321",
    "6e3abeb2-507d-4893-8210-1af1302c7371",
    "6ae33071-722f-467a-917e-ee9df57bbeef",
    "536f0061-4f51-4684-b7bd-01c4cd004fb7",
    "56fd8d73-7b85-40f9-bd10-9536f29b3cb6",
    "1f14c95f-194d-4ff9-a1f2-c7696f75878f",
    "3633c8f7-afa9-48a7-8206-ca7fa9829e35",
    "c298f0bf-34fe-4a6e-97ca-d705b8f83c05",
    "81e70c54-1d40-43f9-bf56-a3e2d379b642",
    "79dc1ded-2ee4-4e29-8d88-babcb30f7c53",
    "e6603e6a-585d-4082-b5fd-35a0cf180b53",
    "9e5734b6-7fab-4067-8496-7fb70cd40c3f",
    "9fe0baea-cd38-4e67-a6ae-364dbcd18965",
    "37b47363-f6ef-4c67-9ada-2b00997a8d98",
    "3ccc8e89-3c0d-4d7d-96a7-9fc3e85706e2",
    "bd49f3ce-27cf-4aed-bc98-73ee82c27907"
  )

  val testUid = "5d1270f0-b974-41ce-a143-4d50c5b0e962"

  val testSetAfter = Set(
    "7308f83b-6ddb-4d5b-90c0-168e450dc2cb",
    "34a946e4-c120-4cfa-9aa1-83178d390c1e",
    "d0212383-89ec-479d-b0b5-a47d9dbe4a23",
    "306c0a55-1c5a-4bb7-89fd-23793b285ed7",
    "68888cc2-4573-444b-adce-4b78a07ef25d",
    "c89c082b-1b86-44c9-93e2-d93fa1e25df0",
    "d98b0395-a5bb-4431-8878-52fd107c1e25",
    "1cad4f41-28bf-4308-b9b4-c8f8396a1c50",
    "37fc6a0d-2f36-4675-b245-c9e5d5afeeff",
    "30869398-ebc2-44b9-87f3-125855faf9ed",
    "43810dd0-bae2-4876-8e20-ada0b990785a",
    "af30f369-434e-4c18-bb35-7aceec820573",
    "456320aa-06bc-4da3-b752-51b5e2a43d59",
    "f342ff5a-cda2-47d4-a9b6-babf9b50248f",
    "ac679b0e-aca9-4a8c-a905-5f6623d9852e",
    "113e0e92-5c34-463b-b19c-c6d621353e1a",
    "adecec00-0fb5-4ca8-bf64-cf6ed70e2781",
    "ab455569-01ff-4672-94ca-d5c1dafb740b",
    "10f468d8-b4db-4a8f-8cca-707aa59b6e7d",
    "45c79dd6-6b14-4345-b5a7-a3c3dfe04c75",
    "f442f310-917c-4e80-b5d9-d4b973562233",
    "dae253b9-2c6f-4c45-963e-1d78c8975e49",
    "d33dd575-5ff2-434a-b54b-dbb080a4d338",
    "baf51240-8baf-4913-8ef1-baff8c97a0b2",
    "5de800e3-9e71-4295-8816-f5497396e2e2",
    "72cb2411-8feb-432d-aa3d-053d88dda553",
    "9a216b0c-8777-43be-b534-e7c474e3e70a",
    "5045573d-e751-4b9c-9d09-81796fb3b9e7",
    "29728ff9-56f5-4914-bc02-97337d4440b0",
    "8307ee46-708d-4cd7-ba97-ccc6d06a93b2",
    "f152fee2-6bbd-4df4-83a8-d290eafbd360",
    "98ab94ab-ac0d-488a-8f67-fe201cf4b9e5",
    "6a3f5af2-f8f0-4a73-afa6-5828cf2c6b8c",
    "2afe005f-b088-44a1-a7e4-a727b18e68dc",
    "d6a6899e-41f0-4c72-9e06-03a0fb2eb03e",
    "03bdba32-1567-4fab-8d66-a8b2d47ebe47",
    "c8404c60-de7d-4ed0-bf20-3e18ab416165",
    "c0f2ba9b-875a-4aac-8470-53de68893d7b",
    "4c875266-7e4d-44b4-aa35-57b72d3b6109",
    "db07201e-c5f9-4a51-99d5-972ca689d281",
    "d9555031-8780-48a2-a5e4-6fa2828b6125",
    "464d0e8c-ec89-4f5c-abfd-e644fa070fd1",
    "3c6725a5-e1c8-4c10-b31e-81670d016c7f",
    "3db5163b-3b2f-4aa2-a360-d80a7d06764a",
    "9fda798e-fe1d-42af-b106-81e323b694de",
    "bd3934bb-f847-4ea9-9c9b-b131f94d0337",
    "caa487cd-043e-4f46-8674-17319170f7b0",
    "5a6c69d9-92ea-4386-8353-84d6d5ff4e45",
    "64632680-8112-4353-a5a6-bc6ce4b70abb",
    "5335cc69-135a-4a15-927e-c2a0a428a97a",
    "03989717-7b06-4215-ae06-c5abdbc10773",
    "2b94209e-6f7b-4625-9d30-0ddf2a87a094",
    "fa0b3ec7-321d-4f4c-b0a5-1ae2237c6e70",
    "b0f14dd0-08dc-4360-9230-76acb7ccedcd",
    "79d5cd27-9d3b-4027-ad3e-fc0911039e8e",
    "2a169ecf-dcb5-49a4-a53e-b7b24f2f5463",
    "293c8932-6f3f-40fb-8119-5322a7f9949e",
    "2191abcd-1d51-4a9d-8c58-6f2c662c5c4d",
    "da00ac96-77ac-49b2-a3e4-6d5355d4a376",
    "567d8e16-25b7-47d7-bce3-aafa0423c5f5",
    "eaed5ad6-33da-4ea2-8626-dee9e5c691e4",
    "9feddcff-afcd-403d-a05e-ba87cab329de",
    "d02df823-563b-461a-8c98-f526af170a28",
    "dc3d74c9-43d7-474c-9d05-010a9fc022b7",
    "5f001668-a2fa-4d5b-9004-1125f2f376d0",
    "7218a301-7d4b-4d40-9cc6-63c7bb6413b4",
    "0d227d4d-ba4b-40e4-b4cf-936e934bd934",
    "2d58eb26-8a35-4f2a-bd7b-701af6b10796",
    "ff859f5b-1f91-473b-beef-a9f86ba33c2f",
    "19aa42d3-d85d-40dd-aebb-82589e8e2fe5",
    "9790cc33-7316-4400-b0e5-13666135596e",
    "7baedbb9-bdf4-4c7b-b9e7-601c344027d3",
    "a1c066e9-e0db-4df3-80dd-33a2571e8691",
    "5efbd85d-9d76-4db5-a3f9-a52f53f5dff9",
    "8b1679fd-0a05-4ae1-962e-bcdf4b04d697",
    "6cabf292-04b4-466f-b098-333672ea5a8b",
    "31be788a-ab12-4f80-87e4-50e309a76db7",
    "e9528e1e-a50a-4b3a-af3b-b7ce3d0a6790",
    "9fdb14d4-296d-472f-869b-2d2636b269b2",
    "83f4b2fe-6844-4834-b244-c456b02b3fe7",
    "618a18b3-1ef5-4fbb-97c8-7ee4deec252c",
    "63ed241c-486b-46da-be50-5394a0b785e1",
    "2422043e-73b9-409e-a47c-9e730dcc0791",
    "819b3629-9c37-4cac-b852-be909e541835",
    "86fad126-f8c2-4d31-8d59-5956de79d32e",
    "f5c5c4b4-f10b-403d-8671-553e3a232d14",
    "92552a76-390e-4c20-ad27-f04449f42432",
    "2fb58bd3-2ce9-4d45-bd14-a24bde06cd2d",
    "c905e126-6365-4fa1-98ee-d3b99ee79432",
    "c6bceb50-725c-4a8d-baf7-c3fd9e615295",
    "aaeeec3e-a42a-43eb-ab90-4c124084f66d",
    "4b780ecf-d91b-4c62-9b14-2aa13a9954af",
    "82ae19b5-602f-4287-99e1-bd00dca1a131",
    "27ccf68b-12f9-4c3f-ac56-e6e86e997e9e",
    "87fbc3b1-a76e-4db0-92cb-abcbbe444666",
    "392baf3e-f769-4ddd-8970-e7318717850d",
    "f09a0753-0f5b-48ac-bd5c-1431b222551a",
    "59f11329-9b30-4950-a846-0c8696b466a7",
    "8070c59f-e7c0-492e-8f29-045fcebae5a1",
    "b172c00e-6cce-4605-a6e3-276bbcf3c776",
    "3cab2cb0-3a38-4013-942f-00b361d27462",
    "ac65c0a0-f95c-445f-bada-2a3c38fb68b9",
    "7e57a53e-1b65-4b1c-90e3-6655edbb8206",
    "0c5897c4-e6aa-40c4-9f4f-4ed18cf96987",
    "1a16e2e7-3b55-49a1-a810-6967c6e8fdd3",
    "77274a70-ed37-4e8b-8025-14997512d786",
    "6d179d08-699b-4621-8508-cbd2cce2a9f6",
    "c1ed2c93-8fd1-4748-ba0c-755dc1265d1d",
    "81c1c4dc-6f32-4d76-bcf8-56cffc62f0fb",
    "29201ec0-6d6e-447d-908a-7cf7abafdca3",
    "d928b600-b8e6-4f6b-b9d9-d12b7db94096",
    "4726051e-903e-48c8-88be-bad8058d251f",
    "da13772c-6942-4285-aa33-4b31f36faa14",
    "7e7bd365-0148-44a6-8e43-c35168aa65b2",
    "47e8bcdc-aac7-4d2c-ac44-3b15550bb34d",
    "e9a99d6d-47b6-48d1-b36a-9244d8ea3ad3",
    "0371e67a-dd18-4aa2-a74b-4278af2fcc2d",
    "bd1a955a-2c89-4734-bd30-465858311ae5",
    "8ef43c19-14f9-41a1-b872-2077db48da30",
    "e994b53a-173d-4134-bacb-5eabd062b2cc",
    "a6472859-c42f-4671-93ea-293aa13191d2",
    "2d66ad35-77c3-45fe-81c3-6f98d72f25b5",
    "6360b769-3847-4f40-b30e-8a4d8c70b3f3",
    "b5f37ead-e5c0-487b-acd3-8cb25fa0a13f",
    "52276ea6-ee46-4493-865d-7b919ee44ea2",
    "7657dd93-12cd-40e4-b24c-bd738b3a5b48",
    "98860e50-6079-42a1-84bd-cf7a18d8ebe3",
    "d0a37b4b-ab46-4b26-a9e1-44b69fa82e29",
    "3577c08b-5a50-402a-9305-dd7bd6b1ef92",
    "5bfbfd45-d387-44c1-af76-1da41d6e4ae9",
    "cd968ae1-3fcd-4620-8ad6-e30f4582271b",
    "78940bc3-8200-43d4-8ecc-0669d9d99125",
    "32bda370-4f18-4b88-b04a-7fafcd97194e",
    "6605be1d-f95e-4f6e-a89e-e6cb1c473375",
    "2ccaf6fa-0f6f-4d9a-a4ba-b22a2b8c26cd",
    "73468080-572e-4a63-a3b6-6c209505d749",
    "bfac5b8a-c854-4698-8b0f-28734e7d10ca",
    "c53c191e-18f6-436f-8eee-8fdc6b3a3ad9",
    "39a60d93-beff-4196-9597-6b74e9cbabe1",
    "6238aa70-a0f0-425e-a103-4a4cb5640651",
    "a465d4ea-eca1-46f9-a41d-c8441c4883a7",
    "35ce1836-48a6-4716-811a-98ee98cadecb",
    "8b7c7dfa-ff6c-44c1-bd06-692fb915a3ad",
    "3b28c43a-57fc-496f-9e34-19088a1a0b42",
    "2089ab1c-c51c-46cf-b60b-4713ce3c1596",
    "5dadc435-70b6-47ad-adff-e2df7088990c",
    "e27eec95-6494-4288-b4a5-fc84fc4a58b2",
    "d784b537-6e0a-4fdd-908c-e824863c7312",
    "9152e60c-590f-466e-9143-7640e73bbd6b",
    "a168aae8-c1c1-4483-883f-e74b9624a11c",
    "07aaa927-af38-4155-ba49-9c8010f559fc",
    "8289d791-b128-4a9e-b1a0-137e35a0e21a",
    "77f5b73c-1294-4111-8ec8-a2c02bacc327",
    "0d30c62f-969a-4094-b0a0-8ff578718e0b",
    "57b20b8c-7c3a-4694-9c6e-d88c652c4730",
    "e37b8d6c-6318-48ab-9d36-7f02003cf4fd",
    "9efb6030-ea86-4f7b-9146-5d9d65bafea9",
    "3d3c2dff-5d3d-44ac-9a51-bfb690e89755",
    "e0aa99e4-15e4-4785-9a8c-7d29f21d206a",
    "78abff28-6783-4ec8-bb74-b27f465b0c37",
    "c84982cf-9fdd-4b60-a0fe-35489cacda9f",
    "4e28a3bd-33da-414e-9795-058053811889",
    "02dd6c56-e8f3-4af3-8ac5-0b4f40520125",
    "4b09a701-6c30-489a-bff8-f2d2cbf5699e",
    "355b5e7a-dc32-485b-b07b-0ac8633b5a83",
    "cc153084-9722-4488-83b1-96089ec774c6",
    "1753337d-b396-46e2-aeea-b283de9fa655",
    "b9a04e9e-897b-4dc7-ab0d-55d47cde1b85",
    "6fa747a6-4dfb-4a92-bd84-66ab974a4d9d",
    "d8a692ab-b321-45b9-a30e-4f04a7c44ca6",
    "8e46f471-077c-405a-b4b9-1153003c6226",
    "147876d4-113f-48ce-b90a-c72cd2a9f457",
    "fc9c9379-3116-4128-b457-9958cee1dba2",
    "7d9f8ddf-a8e8-48a2-b78f-46fcb75029ae",
    "576457f5-ff75-4500-a2fb-2b64ddf3e9e0",
    "d7de914d-8c8a-4fab-8496-3f8fa9ea624b",
    "f67259b0-0a96-4a04-bd43-646d4a7e6f02",
    "3f915f70-1c4c-4710-bed1-bcfc1eb7a99b",
    "e5c73018-5328-4ad1-a38f-56e0cd2889e4",
    "1fda654b-8b96-4107-8783-58195adc3d04",
    "e6dca059-e9dc-4fa1-aed3-ce9a1b1ad798",
    "dee99675-b6ab-4ee6-a0a2-d9880e12c324",
    "38f865a7-e461-4d9e-9d5b-fb658a3d6bcd",
    "b5c047e0-fd42-4f39-a5a4-4b31b184253a",
    "ab062d73-275a-459a-b224-3425c7e0e00e",
    "49c8bcdd-3354-4535-9f70-a571bfcd4a5a",
    "037d2bba-a86a-476f-8994-9d786ee34124",
    "53b793f3-2de9-4256-9e58-060983b7d29c",
    "dc0c983f-77bf-4acd-823d-6cff0b49d5cf",
    "ef94d45e-9ec8-4a2b-b2d5-f0b2099b7eee",
    "bd280687-a045-44f6-b56c-9c8d017fd977",
    "ff5639c5-bc26-4301-9a4d-d2396d53fe5f",
    "a2c95e89-48e1-42d3-82fe-15318381fb00",
    "f49f034d-b912-4030-aac3-7d85db2fff52",
    "7119a2cb-862b-45c2-8928-6a5ae3213550",
    "7fd614b8-4e7f-467a-9a14-f58129c4badf",
    "7b586270-6752-425f-9a53-561d8f8782cc",
    "f3a1ce8c-3853-4b32-9f72-0de41c43a1c4",
    "3f83818a-b205-4ca1-8f4f-d40c614b165f",
    "ee686cc1-97b4-4256-91f0-1b8ac620cf7b",
    "c781b341-bb3e-4f72-a154-c164e645e11d",
    "9e034219-f527-4495-9b7f-8763e2c96bcd",
    "3bd04cdc-ed36-4a76-b9fc-e8938779f2a2",
    "13e7641d-d902-4c8f-9391-8bb3ff6b697c",
    "0310b854-9fa5-49a7-bfa4-e7a49fbe28b7",
    "7de7a98b-845a-434f-8f94-ed76625f953f",
    "28ce74a3-3ce7-4df6-b35d-477913d4fced",
    "6662059f-355e-45fb-a809-f0f9efa01a30",
    "5781033f-5fcf-4580-ba81-198fe93fd428",
    "b26b80b8-af7a-46c4-86af-ba29f3151b81",
    "88d1c04e-4f01-4e5d-a5ca-6cbf11953548",
    "8e13338e-554a-4731-9d43-9470b6e7d627",
    "04f3f82f-39ff-47d2-ab8f-b38d5a7d7f93",
    "e32afaf3-322f-4253-b9c5-913b20ea57b0",
    "f538c1a7-feb9-4ae0-bb2b-1e5f4c9c3301",
    "9088de84-7621-4aa6-b5b9-0c2674911c80",
    "dde0d580-a62b-4d31-99c6-610f3091cc3c",
    "1d2d9ffb-33ba-44fd-8304-642d3e1b9f86",
    "ef5369ad-1961-40b3-9c3d-c1e3a35f5bc1",
    "c4e2ee47-85ab-4db4-81d9-cbefdd0cc318",
    "a575b63b-749d-43ff-aeb3-1c1f941314b5",
    "cb994c4a-d476-427b-81ae-e3df2511c025",
    "1e07d1f3-8c16-4881-9793-e2051651c430",
    "11f8167c-33a5-4498-a356-588e1c59c075",
    "4215a7af-f1d8-4a56-8397-8f35cd5f17ea",
    "cf0b268f-6695-4caf-bd37-0bbe24070596",
    "e80f582b-1d78-42e2-bb1a-7837c32b8be0",
    "374c53eb-9c0e-4557-b334-c588566e9cac",
    "2e7571e3-33db-4f2d-b6f5-e6e919c37cbe",
    "9eddaf95-f3cc-4dfd-8f75-182acf27f5d0",
    "69d8b39c-5120-49c1-8e60-bde9d0270e6d",
    "d08df6ab-b2dd-4a5d-9606-64beb8615252",
    "e98d02e2-cd87-4364-82bb-68b076fef688",
    "c2448ecb-bedf-4500-b6f0-ea2ae33a89f6",
    "6f264883-1a3a-4cc9-9fe4-ddd7caf8d9ec",
    "76a321f5-3954-4f32-85b0-bc3a14d80748",
    "fd03278a-ee94-4543-9734-29c38c441faf",
    "f8a9b445-825c-4a65-b6af-3866702622ec",
    "4209269a-88c2-4cb5-b318-71890b8e3503",
    "39db49e7-c379-41a3-a459-825950b8bc0f",
    "7349c377-ccb6-49a0-ac72-de4abc903773",
    "e12ad04b-6218-42af-bcc7-23f4919e288c",
    "6de315f9-1a0b-4a0a-a090-f0b3256efc37",
    "19fd1f97-108e-4a91-8586-e15809ffa470",
    "4281e61a-903f-48bd-b67a-ebb56a3b2854"
  )

  val testSetComplete = testSetBefore ++ testSetAfter + testUid
}

