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

class CheckNonEmpty(
  val field: String,
  val onPass: ActionList = new ActionList,
  val onFail: ActionList = new ActionList
) extends Action {

  ValidString(field).getOrElse(throw new Exception("""CheckNonEmpty: field was nonexistent or empty ("")"""))

  // must have onPass or onFail, otherwise what's the point?
  if (onPass.actions.isEmpty && onFail.actions.isEmpty) throw new Exception("""CheckNonEmpty: expected at least one action in onPass or onFail""")

  /** JSON constructor 
    */
  def this(config: JValue, actionFactory: Option[ActionFactory]) = {
    this(
      JsonUtil.extractOptionString(config \ "field").getOrElse(throw new Exception("""CheckNonEmpty:  JSON configuration did not have a 'field' object""")),
      new ActionList(config \ "onPass", actionFactory),
      new ActionList(config \ "onFail", actionFactory)
    )
  }

  /** toString
    */
  override def toString() = {
    
    val sb = new StringBuffer
    sb.append(s"""CheckNonEmpty:{""")
    sb.append(s"""field = ${field}""")
    sb.append(s""", onPass = ${onPass.toString}""")
    sb.append(s""", onFail = ${onFail.toString}""")
    sb.append("}")
    sb.toString
  }

  /** Perform the action
    *
    */
  def perform(
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    PerformResult = {

    implicit val jsonFormats = org.json4s.DefaultFormats

    // get the test value
    val testVal = ValidString(JsonUtil.extractOptionString(inputRecord \ field))

    // If found, it exists and is a nonzero length string; it passes.  Run onPass.
    if (testVal.nonEmpty) {
      onPass.perform(inputRecord, currentEnrichedMap, context)
    }
    // If not found or is equal to "", it fails.  Run onFail.
    else {
      onFail.perform(inputRecord, currentEnrichedMap, context)
    }
  }

}



