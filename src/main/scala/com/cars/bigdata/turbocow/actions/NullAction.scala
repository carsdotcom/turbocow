package com.cars.bigdata.turbocow.actions

import com.cars.bigdata.turbocow.Action
import com.cars.bigdata.turbocow.ActionContext
import com.cars.bigdata.turbocow.JsonUtil
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.JsonAST.JNothing
import org.json4s.jackson.JsonMethods._
import com.cars.bigdata.turbocow.ActionFactory
import com.cars.bigdata.turbocow.PerformResult
import com.cars.bigdata.turbocow._

import scala.io.Source

// Action that does nothing.
class NullAction(
  // name may be useful for testing, but doesn't have to be used.
  val name: Option[String] = None
) extends Action {

  var wasRun = false

  /** Json constructor
    * 
    */
  def this(config: JValue) {
    this(
      JsonUtil.extractOptionString(config \ "name")
    )
  }

  /** Perform the action
    *
    */
  def perform(
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    PerformResult = {

    // mark this as run (helpful in testing)
    wasRun = true

    PerformResult()
  }

}




