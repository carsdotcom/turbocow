package com.cars.ingestionframework

import org.json4s._

class AddEnrichedFields(actionConfig: JValue) extends Action
{
  implicit val jsonFormats = org.json4s.DefaultFormats

  // create list of key-value tuples from the config
  val kvList = actionConfig.children.map{ jObj => 
    ( (jObj \ "key").extract[String], (jObj \ "value").extract[String] )
  }

  /** Perform the action
    */
  def perform(sourceFields: List[String], inputRecord: JValue, currentEnrichedMap: Map[String, String]): 
    Map[String, String] = {

    // Add the kvList to the enriched data
    kvList.toMap
  }
}



