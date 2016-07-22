package com.cars.bigdata.turbocow

import org.json4s.JValue

class Action2 extends Action
{
  /** Perform the action
    */
  def perform(
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    PerformResult = {

    PerformResult()
  }
}


