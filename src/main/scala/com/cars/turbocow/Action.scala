package com.cars.turbocow

import org.json4s.JsonAST.JValue

trait Action extends Serializable
{
  /** Performs an action.  
    *
    * @param sourceFields the list of fields from the "source" array for this item.
    * @param inputRecord the parsed JSON record received as input
    * @param currentEnrichedMap the map that represents the entire enriched data 
    *        record so far in this action sequence.
    * @param context the runtime context this action is being invoked in.
    * 
    * @return key-value map that will be added to the currentEnrichedMap 
    *         before the next call.  
    */
  def perform(
    sourceFields: List[String], 
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    PerformResult
}

/** This is what is returned from perform()
  */
case class PerformResult(
  enrichedUpdates: Map[String, String] = Map.empty[String, String],
  rejectionReason: Option[String] = None
)


