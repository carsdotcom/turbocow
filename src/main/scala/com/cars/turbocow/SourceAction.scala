package com.cars.turbocow

import scala.collection.immutable.HashMap
import org.json4s.JsonAST.JValue

import scala.annotation.tailrec

// TODO rename this to "Item"
case class SourceAction(
  actions: List[Action],
  name: Option[String] = None
) extends Action
{
  /** Run through all actions and perform each in order.
    */
  override def perform(
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    PerformResult = {

    var enrichedMap = currentEnrichedMap
    
    /** Recursively process an action list.  Allows for short-circuiting due to
      * returning stopProcessingActionList==true.
      * @todo this could be reused elsewhere too.  add to Action?  (or new trait)
      */    
    @tailrec
    def recursivePerform(
      actions: List[Action],
      prevResult: PerformResult,
      inputRecord: JValue, 
      context: ActionContext): 
      PerformResult = {

      val action = actions.headOption
      if (action.isEmpty) {
        prevResult
      }
      else {
        val result = action.get.perform(inputRecord, prevResult.enrichedUpdates, context)

        val actionsLeft = 
          if (result.stopProcessingActionList) List.empty[Action]
          else actions.tail

        recursivePerform(actionsLeft, prevResult.combineWith(result), inputRecord, context)
      }
    }
    recursivePerform(actions, PerformResult(), inputRecord, context)    
  }
}
