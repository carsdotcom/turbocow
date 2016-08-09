package com.cars.bigdata.turbocow.actions.checks

import com.cars.bigdata.turbocow.actions.checks.BinaryCheck.caseSensitiveDefault
import com.cars.bigdata.turbocow.{ActionContext, FieldSource}
import org.json4s.JValue

class EqualChecker(val caseSensitive : Boolean = caseSensitiveDefault) extends Checker {

  /** Check if the two fields are equals or not .
    */
  def performCheck(
    checkParams: CheckParams,
    inputRecord: JValue,
    currentEnrichedMap: Map[String, String],
    context: ActionContext): Boolean = {

    if (checkParams.right.isEmpty)
      false
    else {
      val leftSource = FieldSource(checkParams.left, checkParams.leftSource.getOrElse(EnrichedThenInput))
      val rightSource = FieldSource(checkParams.right, checkParams.rightSource.getOrElse(EnrichedThenInput))

      val leftIsNull = leftSource.isValueNull(inputRecord, currentEnrichedMap, context.scratchPad)
      val rightIsNull = rightSource.isValueNull(inputRecord, currentEnrichedMap, context.scratchPad)
      if ( leftIsNull && rightIsNull )
        true // equal if both are null
      else if (leftIsNull || rightIsNull)
        false // non-null and null are always not equal
      else {
        // Both left and right are non-null
        // It's okay if they are missing, getValue() will return None
        val leftVal = leftSource.getValue(inputRecord, currentEnrichedMap, context.scratchPad)
        val rightVal = rightSource.getValue(inputRecord, currentEnrichedMap, context.scratchPad)

        if (leftVal.nonEmpty && rightVal.nonEmpty) {
          if (caseSensitive)
            leftVal.equals(rightVal)
          else
            leftVal.get.toLowerCase.equals(rightVal.get.toLowerCase)
        }
        else
          false // one or more of left & right are nonexistent... always nonequal
      }
    }
  }
}



