package com.cars.bigdata.turbocow.actions

import com.cars.bigdata.turbocow._
import org.json4s.JsonAST.JValue

class MockAction(
  val shouldThrow: Boolean = false,
  val throwMessage: Option[String] = None
) extends Action
{
  def this(config: JValue) = {
    this(
      JsonUtil.extractOption[Boolean](config \ "shouldThrow").getOrElse(false),
      JsonUtil.extractOptionString(config \ "throwMessage")
    )
  }

  /** Perform the action
    */
  def perform(
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext):
    PerformResult = {

    if (shouldThrow) {
      throw new Exception("MockAction:  " + throwMessage.getOrElse("throwing exception"))
    }
    else {
      PerformResult()
    }
  }
}




