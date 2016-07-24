package com.cars.bigdata.turbocow.actions.checks

import com.cars.bigdata.turbocow.actions.{CheckParams, Checker}
import com.cars.bigdata.turbocow.{ActionContext, JsonUtil, ValidString}
import org.json4s.JValue

class NumericChecker extends Checker {

  /** Check if the requested field is numeric
    */
  def performCheck(
    checkParams: CheckParams,
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    Boolean = {

    // get the test value
    val testVal = ValidString(JsonUtil.extractOptionString(inputRecord \ checkParams.left))
    testVal.isEmpty
  }
}


