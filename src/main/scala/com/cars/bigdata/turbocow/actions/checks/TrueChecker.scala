package com.cars.bigdata.turbocow.actions.checks

import com.cars.bigdata.turbocow.{ActionContext, JsonUtil, ValidString}
import org.json4s.JValue

class TrueChecker extends Checker {

  /** Check if the requested field is numeric
    */
  def performCheck(
    checkParams: CheckParams,
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    Boolean = {

    // get the test value
    val testVal = JsonUtil.extractValidString(inputRecord \ checkParams.left)
    if(testVal.isDefined) {
      testVal.get.toLowerCase.equals("true")
      //todo Optional Source (input | enriched | constant) handling
      //todo extend to accept boolean type values if needed. for now we are checking "true" as a String but not as boolean.
    }
      else{
      return false
    }
  }
}


