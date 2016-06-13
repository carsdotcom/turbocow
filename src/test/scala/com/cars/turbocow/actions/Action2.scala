package com.cars.turbocow

import org.json4s.JsonAST.JValue

class Action2 extends Action
{
  /** Perform the action
    */
  def perform(
    sourceFields: List[String], 
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    Map[String, String] = {

    Map.empty[String, String]
  }
}


