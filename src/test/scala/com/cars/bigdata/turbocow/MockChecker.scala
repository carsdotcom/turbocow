package com.cars.bigdata.turbocow.actions

import com.cars.bigdata.turbocow.ActionContext
import com.cars.bigdata.turbocow.actions.checks.{CheckParams, Checker}
import org.json4s.JValue

class MockChecker extends Checker {

  var performCount = 0

  /** Check if the requested field is not empty.
    */
  def performCheck(
    checkParams: CheckParams,
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    Boolean = {

    performCount += 1

    return true
  }
}



