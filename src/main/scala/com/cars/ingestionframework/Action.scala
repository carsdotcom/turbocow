package com.cars.ingestionframework

import org.json4s.JsonAST.JValue

trait Action
{
  /** Performs an action.  
    *
    * @param sourceFields the list of fields from the "source" array for this item.
    * @param inputRecord the parsed JSON record received as input
    * @param currentEnrichedMap the map that represents the entire enriched data 
    *        record so far in this action sequence.
    * 
    * @return key-value map that will be added to the currentEnrichedMap 
    *         before the next call.  
    */
  def perform(sourceFields: List[String], inputRecord: JValue, currentEnrichedMap: Map[String, String]): 
    Map[String, String] 

}

