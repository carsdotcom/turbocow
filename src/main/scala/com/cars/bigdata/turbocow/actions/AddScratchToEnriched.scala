package com.cars.bigdata.turbocow.actions

import com.cars.bigdata.turbocow.{Action, ActionContext, JsonUtil, PerformResult}
import org.json4s.JsonAST.JValue

class AddScratchToEnriched(keyArray: List[String]) extends Action
{
  def this(actionConfig: JValue) = {
    this(
      keyArray = (actionConfig \ "fields").children.map(eachKey => JsonUtil.extractString(eachKey))
    )
  }
  /** Perform the action
    */
  def perform(
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext):
    PerformResult = {

  // How do wrap in Option to avoid null checks on context.scratchPad.get("dateId")
  PerformResult(
    keyArray.map { eachKey =>
      val getVal: Option[Any] = context.scratchPad.get(eachKey)
      if (getVal isDefined) {
        // Just add this to the enriched data - doesn't matter
        Map(eachKey -> getVal.get.toString)
      } else {
        Map.empty[String, String]
      }
    }.reduce(_ ++ _)
  )
  }
}



