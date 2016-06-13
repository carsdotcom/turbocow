package com.cars.turbocow

import org.json4s.JsonAST.JValue

class Custom1 extends Action
{
  /** Perform the action
    */
  def perform(
    sourceFields: List[String], 
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    Map[String, String] = {

    // Just add this to the enriched data - doesn't matter
    Map("CUSTOM1-KEY"->"CUSTOM1-VALUE")
  }
}



