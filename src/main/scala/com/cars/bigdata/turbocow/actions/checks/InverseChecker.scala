package com.cars.bigdata.turbocow.actions.checks

import com.cars.bigdata.turbocow.ActionContext
import org.json4s.JValue

class InverseChecker(val checker: Checker)
  extends Checker {

  /** Check if the requested field is not empty.
    */
  def performCheck(
    checkParams: CheckParams,
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    Boolean = {

    !(checker.performCheck(checkParams, inputRecord, currentEnrichedMap, context))
  }
}



