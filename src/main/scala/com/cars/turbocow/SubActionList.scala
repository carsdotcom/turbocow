package com.cars.turbocow.actions

import com.cars.turbocow.{Action, ActionFactory, JsonUtil, PerformResult}

import org.json4s._
import org.json4s.jackson.JsonMethods._

class SubActionList(
  val actions: List[Action] = List.empty[Action]
)
extends Serializable //with Action
{
  /** create actions list for sub actions.
    * You MUST have an actionFactory if parsing actions in onPass/onFail.  
    * Only pass in None if you are running a test.
    */
  def this(
    jval: JValue, 
    actionFactory: Option[ActionFactory],
    sourceFields: List[String] = List.empty[String]) = {
    
    this(
      //reject = JsonUtil.extractOptionalBool((jval \ "reject"), false),
      actions = {
        val jActions = jval.toOption
        // if no actions, return empty list
        if (jActions.isEmpty) List.empty[Action]
        // otherwise, must have an action factory to parse the actions
        else { 
          if (actionFactory.isEmpty) throw new Exception("Must include an ActionFactory to Lookup constructor when using onPass/onFail.")
          else actionFactory.get.createActionList(jActions.get, sourceFields)
        }
      }
    )
  }

  /** Performs all actions.  
    *
    */
    // see 
//  def perform(
//    sourceFields: List[String], 
//    inputRecord: JValue, 
//    currentEnrichedMap: Map[String, String],
//    context: ActionContext): 
//    PerformResult = {
//
//    PerformResult( 
//      actions.map{ action => 
//        action.perform(sourceFields, inputRecord, currentEnrichedMap, context).enrichedUpdates.toList
//      }.foldLeft( List.empty[Tuple2[String, String]] )( _ ++ _ )
//    )
//  }
}

