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
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    PerformResult

  // TODO - Nageswar
  //def getEnrichedOrInputField(fieldName: String, inputRecord: JValue, enrichedMap: Map[]): String = { } 

  /** All actions need to be able to return their table-caching needs for 
    * cached lookups.  The default is to return nothing.
    */
  def getLookupRequirements: Option[CachedLookupRequirement] = None
}

/** This is what is returned from perform()
  */
case class PerformResult(
  // The updates to the enriched map.  Any keys specified will overwrite the 
  // current keys in the map.
  enrichedUpdates: Map[String, String] = Map.empty[String, String],

  // If true, the engine will short-circuit and exit the processing of this action list,
  // moving on to the next.
  stopProcessingActionList: Boolean = false
)
{

  /** Combine this PerformResult with another.  (merges the maps together, 
    * and if stopProcessing is true, keeps it true.)
    */
  def combineWith(other: PerformResult): PerformResult = {
    PerformResult(
      enrichedUpdates ++ other.enrichedUpdates, 
      stopProcessingActionList || other.stopProcessingActionList)
  }
}


