package com.cars.bigdata.turbocow.actions.checks

import com.cars.bigdata.turbocow.{ActionContext, JsonUtil}
import org.json4s.{JValue, JsonAST}

class EqualChecker extends Checker {

  /** Check if the two fields are equals or not .
    */
  def performCheck(
                    checkParams: CheckParams,
                    inputRecord: JValue,
                    currentEnrichedMap: Map[String, String],
                    context: ActionContext): Boolean = {

    val leftVal = inputRecord \ checkParams.left
    val rightOption: Option[String] = checkParams.right
    if (rightOption.isEmpty) {
      return false
    }

    val rightVal = inputRecord \ rightOption.get

    if (leftVal.isInstanceOf[JsonAST.JNothing.type] && rightVal.isInstanceOf[JsonAST.JNothing.type]) {
      return false
    }

    leftVal.equals(rightVal)
  }

}


