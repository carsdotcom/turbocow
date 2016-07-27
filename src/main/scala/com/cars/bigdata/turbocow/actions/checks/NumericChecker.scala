package com.cars.bigdata.turbocow.actions.checks

import com.cars.bigdata.turbocow.actions.{CheckParams, Checker}
import com.cars.bigdata.turbocow.{ActionContext, JsonUtil}
import org.json4s.JValue
import scala.util.{Try, Success, Failure}

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
    val testVal = JsonUtil.extractValidString(inputRecord \ checkParams.left)
    if(testVal.isDefined) {
      //just making sure converting is sucessful without any exceptions and doesn't contain double float identifiers at the end
      Try{testVal.get.toDouble}.isSuccess && !List( 'd', 'D', 'f', 'F').contains(testVal.get.trim.last)
    }
    else{
      false
    }
  }
}


