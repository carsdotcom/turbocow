package com.cars.bigdata.turbocow

import org.json4s.JValue

class Custom1 extends Action
{
  /** Perform the action
    */
  def perform(
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    PerformResult = {

    // Just add this to the enriched data - doesn't matter
    PerformResult(Map("CUSTOM1-KEY"->"CUSTOM1-VALUE"))
  }
}



