package com.cars.ingestionframework

import org.json4s.JsonAST.JValue

trait Action
{
  /** Performs an action.  
    *
    * @return key-value map that will be added to the currentEnrichedMap 
    *         before the next call.  
    */
  def perform(sourceFields: List[String], sourceJson: JValue, currentEnrichedMap: Map[String, String]): 
    Map[String, String] 

}

