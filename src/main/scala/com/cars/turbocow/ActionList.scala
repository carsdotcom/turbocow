package com.cars.turbocow.actions

import com.cars.turbocow._
import org.json4s._

import scala.annotation.tailrec

class ActionList(
  val actions: List[Action] = List.empty[Action]
)
extends Action 
with Serializable
{
  /** create actions list for sub actions.
    * You MUST have an actionFactory if parsing actions in onPass/onFail.  
    * Only pass in None if you are running a test.
    */
  def this(
    jval: JValue, 
    actionFactory: Option[ActionFactory]) = {
    
    this(
      //reject = JsonUtil.extractOptionalBool((jval \ "reject"), false),
      actions = {
        val jActions = jval.toOption
        // if no actions, return empty list
        if (jActions.isEmpty) List.empty[Action]
        // otherwise, must have an action factory to parse the actions
        else { 
          if (actionFactory.isEmpty) throw new Exception("Must include an ActionFactory to Lookup constructor when using onPass/onFail.")
          else actionFactory.get.createActionList(jActions.get)
        }
      }
    )
  }

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
    recursivePerform(actions, PerformResult(currentEnrichedMap), inputRecord, context)
  }
}

