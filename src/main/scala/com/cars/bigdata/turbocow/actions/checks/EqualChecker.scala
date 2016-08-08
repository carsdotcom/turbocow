package com.cars.bigdata.turbocow.actions.checks

import com.cars.bigdata.turbocow.{ActionContext, FieldSource, JsonUtil}
import org.json4s.JValue
import org.json4s.JsonAST.{JNothing, JNull}
import BinaryCheck.caseSensitiveDefault

class EqualChecker(val caseSensitive : Boolean = caseSensitiveDefault) extends Checker {

  /** Check if the two fields are equals or not .
    */
  def performCheck(
    checkParams: CheckParams,
    inputRecord: JValue,
    currentEnrichedMap: Map[String, String],
    context: ActionContext): Boolean = {

    val leftJValue = inputRecord \ checkParams.left
    val leftOption : Option[String] = Option(checkParams.left)
    val rightOption: Option[String] = checkParams.right
    if (rightOption.isEmpty) {
      return false
    }
    val rightJValue = inputRecord \ checkParams.right.get
    if(caseSensitive){
      // when caseSensitive is set to true or not given in config.
      // returns false for "abcd" and "ABcD" comparison
      //grab operand1 value from input / enriched/ as a constant(direct check of that value)
      val leftVal = checkParams.leftSource match {
        case Some(FieldSource.Input) => JsonUtil.extractValidString(leftJValue)
        case Some(FieldSource.Enriched) => currentEnrichedMap.get(checkParams.left)
        case Some(FieldSource.Constant) => Option(checkParams.left)
        case None => JsonUtil.extractValidString(leftJValue)
        case a: Any => throw new Exception("unrecognized field source:" + a.toString)
      }
      //grab operand2 value from input / enriched/ as a constant(direct check of that value)
      val rightVal = checkParams.rightSource match {
        case Some(FieldSource.Input) => if(leftJValue == JNothing && rightJValue == JNothing){
          return false
        }
          if(leftJValue == JNull && rightJValue == JNull){
            return true
          }
          if(leftJValue == JNull || rightJValue == JNull){
            return false
          }
          JsonUtil.extractValidString(rightJValue)
        case Some(FieldSource.Enriched) => currentEnrichedMap.get(rightOption.get)
        case Some(FieldSource.Constant) => rightOption
        case None => JsonUtil.extractValidString(rightJValue)
        case a: Any => throw new Exception("unrecognized field source:" + a.toString)
      }
      leftVal.equals(rightVal)
    }
    else {
      // when caseSensitive is set to false
      /// returns true for "abcd" and "ABcD" comparison
      //grab operand1 value from input / enriched/ as a constant(direct check of that value)
      val leftVal = checkParams.leftSource match {
        case Some(FieldSource.Input) =>
          if(leftJValue == JNothing && rightJValue == JNothing){
            return false
          }
          if(leftJValue == JNull && rightJValue == JNull){
            return true
          }

          if(leftJValue == JNull || rightJValue == JNull){
            return false
          }

          if(leftJValue == JNothing || rightJValue == JNothing){
            return false
          }
          JsonUtil.extractValidString(leftJValue).get.toLowerCase
        case Some(FieldSource.Enriched) => // when source is from enriched
          if(! currentEnrichedMap.contains(leftOption.get) && !currentEnrichedMap.contains(rightOption.get)){
            return true
          }
          if(! currentEnrichedMap.contains(leftOption.get) || !currentEnrichedMap.contains(rightOption.get) ){
            return false
          }
          if(currentEnrichedMap(leftOption.get) == currentEnrichedMap(rightOption.get)){
            return true
          }
          if(currentEnrichedMap.contains(leftOption.get)){
          currentEnrichedMap(leftOption.get).toLowerCase
        }
        case Some(FieldSource.Constant) => checkParams.left.toLowerCase
        case None => JsonUtil.extractValidString(leftJValue).get.toLowerCase
        case a: Any => throw new Exception("unrecognized field source:" + a.toString)
      }

      //grab operand2 value from input / enriched/ as a constant(direct check of that value)
      val rightVal = checkParams.rightSource match {
        case Some(FieldSource.Input) => JsonUtil.extractValidString(rightJValue).get.toLowerCase
        case Some(FieldSource.Enriched) =>if(currentEnrichedMap.contains(rightOption.get)){
          currentEnrichedMap(rightOption.get).toLowerCase
        }
        case Some(FieldSource.Constant) => rightOption.get.toLowerCase
        case None => JsonUtil.extractValidString(rightJValue).get.toLowerCase
        case a: Any => throw new Exception("unrecognized field source:" + a.toString)
      }
      leftVal.equals(rightVal)
    }
  }
}


