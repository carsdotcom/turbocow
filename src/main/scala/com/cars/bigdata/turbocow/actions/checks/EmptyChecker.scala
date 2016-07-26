package com.cars.bigdata.turbocow.actions

import com.cars.bigdata.turbocow.{ActionContext, JsonUtil, ValidString}
import org.json4s.JValue

class EmptyChecker extends Checker {

  /** Check if the requested field is not empty.
    */
  def performCheck(
    checkParams: CheckParams,
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    Boolean = {

    // get the test value
    val testVal = ValidString(checkParams.getLeftValue(inputRecord, currentEnrichedMap))
    val result = (testVal.isEmpty || testVal.get.isEmpty)
    result
  }
}


