package com.cars.ingestionframework

import org.json4s.JsonAST.JValue

class Action2 extends Action
{
  /** Perform the action
    */
  def perform(sourceFields: List[String], sourceJson: JValue, currentEnrichedMap: Map[String, String]): 
    Map[String, String] = {

    Map.empty[String, String]
  }
}


