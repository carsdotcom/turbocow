package com.cars.bigdata.turbocow.actions

import com.cars.bigdata.turbocow.{Action, ActionContext, JsonUtil, PerformResult}
import org.json4s.JsonAST.JValue
import org.json4s._

class AddScratchToEnriched(val keys: List[String]) extends Action
{
  //Check keyArray inputs
  if (keys.isEmpty) throw new Exception("keyArray is emptly")
  for (key <- keys) {
    if (key.trim.isEmpty) throw new Exception("keyArray fields are empty or blank strings")
  }

  /**
    * Secondary JSON constructor
    * @param actionConfig
    */
  def this(actionConfig: JValue) = {
    this(
      actionConfig.toOption match {
        case None => throw new Exception("'config' section is missing in 'AddScratchToEnriched' action.")
        case Some(actionConfig) => {
          val fields = (actionConfig \ "fields")
          fields match {
            case jA: JArray => ;
            case _ => throw new Exception("Fields isn't a JArray")
          }
          if (fields.children.isEmpty) throw new Exception("fields array is empty")
          val keys = (fields.children.map(eachKey => JsonUtil.extractOptionString(eachKey).get.trim))
          for (key <- keys) {
            if (key.isEmpty) throw new Exception("key in fields array is empty")
          }
          keys
        }
      }
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
    keys.map { eachKey =>
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



