package com.cars.ingestionframework

import scala.collection.immutable.HashMap
import org.json4s.JsonAST.JValue

case class SourceAction(
  source: List[String], 
  actions: List[Action]
) extends Action
{
  /** Run through all actions and perform each in order.
    */
  def perform(
    sourceFields: List[String], 
    sourceJson: JValue, 
    currentEnrichedMap: Map[String, String]): 
    Map[String, String] = {

    var enrichedMap = currentEnrichedMap
    
    actions.foreach{ action => 
      val mapAddition = action.perform(sourceFields, sourceJson, enrichedMap)

      // merge in the results
      enrichedMap = enrichedMap ++ mapAddition
    }
    
    // (todo) there's a better way to do this than foreach...
    
    // return the aggregate of all things actions:
    enrichedMap
  }
}
