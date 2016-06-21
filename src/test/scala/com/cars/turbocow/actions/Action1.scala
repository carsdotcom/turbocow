package com.cars.turbocow

import org.json4s.JsonAST.JValue

class Action1 extends Action
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


