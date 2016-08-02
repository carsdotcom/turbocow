package com.cars.bigdata.turbocow.actions.checks

import com.cars.bigdata.turbocow.ActionContext
import org.json4s.JValue

trait Checker extends Serializable {

  /** Method to perform the check.
    */
  def performCheck(
    checkParams: CheckParams,
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    Boolean 
}

