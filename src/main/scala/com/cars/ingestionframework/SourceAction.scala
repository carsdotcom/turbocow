package com.cars.ingestionframework

import scala.collection.immutable.HashMap

case class SourceAction(
  source: String, 
  actions: List[Action]
) extends Action

  /** Run through all actions and perform each in order.
    */
  def perform(
    field: List[String], 
    sourceJson: JValue, 
    currentEnrichedMap: Map[String, String]): 
    Map[String, String] = {

    var enrichedMap = currentEnrichedMap

    actions.foreach{ action => 
      val mapAddition = action.perform(field, sourceJson, enrichedMap)

      // merge in the results
      enrichedMap = enrichedMap.merged(mapAddition)
    }

    // (todo) there's a better way to do this than foreach...

    // return the aggregate of all things actions:
    enrichedMap
  }
)
