package com.cars.bigdata.turbocow

import com.cars.bigdata.turbocow.actions.ActionList
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.hive.HiveContext
import org.json4s._
import org.json4s.jackson.JsonMethods._

case class EngineBroadcasts(
  itemsBC: Broadcast[List[Item]],
  exceptionHandlingActionsBC: Broadcast[ActionList],
  tableCachesBC: Broadcast[Map[String, TableCache]],
  initialScratchPadBC: Broadcast[ScratchPad],
  jdbcClientConfigsBC: Broadcast[Seq[JdbcClientConfig]]
)

object EngineBroadcasts
{

  /** Create a EngineBroadcasts object.  Use with care as this will actually
    * perform the broadcast.
    * 
    * @param config configuration string that describes how to process the input data
    * @param sc SparkContext
    * @param hiveContext the hive context, if any
    * 
    */
  def create(
    config: String, 
    sc: SparkContext,
    hiveContext : Option[HiveContext] = None,
    actionFactory: ActionFactory = new ActionFactory,
    initialScratchPad: ScratchPad = new ScratchPad,
    jdbcClientConfigs: Seq[JdbcClientConfig] = Nil
  ): EngineBroadcasts = {

    // Create the list of Items from the config and broadcast
    val parsedConfig = parse(config)
    val items = actionFactory.createItems(parsedConfig)
    val itemsBC: Broadcast[List[Item]] = sc.broadcast(items)

    // Create the exceptionHandling from the config:
    val exceptionHandlingActions = new ActionList(
      actionFactory.createActionList(
        ( parsedConfig \ "global" \ "exceptionHandlingList" ).toOption
      )
    )
    val exceptionHandlingActionsBC = sc.broadcast(exceptionHandlingActions)

    // Cache all the tables as specified in the items, then broadcast
    val tableCaches: Map[String, TableCache] =
      HiveTableCache.cacheTables(items, hiveContext)
    val tableCachesBC = sc.broadcast(tableCaches)

    // Broadcast some other things
    val initialScratchPadBC = sc.broadcast(initialScratchPad)
    val jdbcClientConfigsBC = sc.broadcast(jdbcClientConfigs)

    // create the object with everything
    EngineBroadcasts(
      itemsBC,
      exceptionHandlingActionsBC,
      tableCachesBC,
      initialScratchPadBC,
      jdbcClientConfigsBC
    )
  }
}

