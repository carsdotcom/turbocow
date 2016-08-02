package com.cars.bigdata.turbocow.actions.checks

import com.cars.bigdata.turbocow.{ActionContext, FieldSource, JsonUtil}
import org.json4s.{JValue, JsonAST}

class EqualChecker extends Checker {

  /** Check if the two fields are equals or not .
    */
  def performCheck(
                    checkParams: CheckParams,
                    inputRecord: JValue,
                    currentEnrichedMap: Map[String, String],
                    context: ActionContext): Boolean = {

    val leftVal = checkParams.leftSource match {
          case Some(FieldSource.Input) => inputRecord \ checkParams.left
          case Some(FieldSource.Enriched) => currentEnrichedMap.get(checkParams.left)
          case Some(FieldSource.Constant) => checkParams.left
          case None => inputRecord \ checkParams.left
          case a: Any => throw new Exception("unrecognized field source:"+ a.toString)
    }

    val rightOption: Option[String] = checkParams.right
    if (rightOption.isEmpty) {
      return false
    }

    val rightVal = checkParams.rightSource match {
      case Some(FieldSource.Input) => inputRecord \ rightOption.get
      case Some(FieldSource.Enriched) => currentEnrichedMap.get(rightOption.get)
      case Some(FieldSource.Constant) => rightOption.get
      case None => inputRecord \ rightOption.get
      case a: Any => throw new Exception("unrecognized field source:"+ a.toString)
    }

    if (leftVal.isInstanceOf[JsonAST.JNothing.type] && rightVal.isInstanceOf[JsonAST.JNothing.type]) {
      return false
    }

    leftVal.equals(rightVal)
  }

}


