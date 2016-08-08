package com.cars.bigdata.turbocow.actions.checks

import com.cars.bigdata.turbocow.{Action, ActionFactory, CachedLookupRequirement}
import com.cars.bigdata.turbocow.actions.ActionList
import org.json4s._

abstract class CheckAction(
  val onPass: ActionList = new ActionList,
  val onFail: ActionList = new ActionList
) extends Action {

  // must have at least one of onPass or onFail; otherwise what's the point?
  if (onPass.actions.isEmpty && onFail.actions.isEmpty) throw new Exception("""CheckNonEmpty: expected at least one action in onPass or onFail""")

  /** Get the lookup requirements
    */
  override def getLookupRequirements: List[CachedLookupRequirement] = {
    onPass.getLookupRequirements ++ onFail.getLookupRequirements
  }

  /** JSON constructor
    */
  def this(config: JValue, actionFactory: Option[ActionFactory]) = {
    this(
      new ActionList(config \ "onPass", actionFactory),
      new ActionList(config \ "onFail", actionFactory)
    )
  }

  /** toString
    */
  override def toString() = {
    
    val sb = new StringBuffer
    sb.append(s"""CheckAction:{""")
    sb.append(s""", onPass = ${onPass.toString}""")
    sb.append(s""", onFail = ${onFail.toString}""")
    sb.append("}")
    sb.toString
  }

}




