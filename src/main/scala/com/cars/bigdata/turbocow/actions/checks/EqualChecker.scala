package com.cars.bigdata.turbocow.actions.checks

import com.cars.bigdata.turbocow.{ActionContext, JsonUtil}
import org.json4s.JValue

class EqualChecker extends Checker {

  /** Check if the requested field is not empty.
    */
  def performCheck(
    checkParams: CheckParams,
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    Boolean = {

    val leftVal = JsonUtil.extractValidString(inputRecord \ checkParams.left)
    val rightOption: Option[String]  = checkParams.right
    if(rightOption.isEmpty){
      false
    }

    val rightVal = JsonUtil.extractValidString(inputRecord \ rightOption.get)

    leftVal.equals(rightVal)
  }
}


