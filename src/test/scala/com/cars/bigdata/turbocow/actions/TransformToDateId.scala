package com.cars.bigdata.turbocow.actions

import com.cars.bigdata.turbocow.{Action, ActionContext, PerformResult, ScratchPad}
import org.json4s.JsonAST.JValue

class TransformToDateId extends Action
{
  /** Perform the action
    */
  def perform(
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext):
    PerformResult = {

  // How do wrap in Option to avoid null checks on context.scratchPad.get("dateId")

    val dateIdOption: Option[Any] = context.scratchPad.get("dateId")
    if(!dateIdOption.isEmpty){
      // Just add this to the enriched data - doesn't matter
      PerformResult(Map("date-id"-> dateIdOption.get.toString))
    }else{
      PerformResult(Map.empty[String, String])
    }


  }
}



