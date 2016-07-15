package com.cars.bigdata.turbocow

import org.json4s._

class AddEnrichedFields(kvList: List[(String, String)]) extends Action
{
  /** Construct with a JValue "config"
    */
  def this(actionConfig: JValue) = this(
    // create list of key-value tuples from the config
    actionConfig.children.map{ jObj =>
      implicit val formats = org.json4s.DefaultFormats
      ( (jObj \ "key").extract[String], (jObj \ "value").extract[String] )
    }
  )

  /** Perform the action
    */
  def perform(
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    PerformResult = {

    // Add the kvList to the enriched data
    PerformResult(kvList.toMap)
  }
}



