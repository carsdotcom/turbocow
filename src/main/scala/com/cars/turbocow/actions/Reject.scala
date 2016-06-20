package com.cars.turbocow.actions

import com.cars.turbocow.Action
import com.cars.turbocow.ActionContext
import com.cars.turbocow.JsonUtil
import com.cars.turbocow.PerformResult
import com.cars.turbocow.ValidString
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.JsonAST.JNothing
import org.json4s.jackson.JsonMethods._

import scala.io.Source

class Reject(
  val reasonFrom: Option[String] = None, 
  val rejectionReason: Option[String] = None)
  extends Action {

  // can't have both:
  if (ValidString(reasonFrom).nonEmpty && ValidString(rejectionReason).nonEmpty) 
    throw new Exception("'reject' actions should not have both 'reason' and 'reasonFrom' fields.  (Pick only one)")

  /** alt constructor with a JValue
    * If there is a configuration section for this reject action, it will 
    * override anything passed in the constructor.
    * 
    * @param  actionConfig the parsed configuration for this action
    */
  def this(actionConfig: JValue) = {

    this(
      reasonFrom = actionConfig match {
        case jobj: JObject => {
          JsonUtil.extractOption[String](jobj \ "reasonFrom")
        }
        case JNothing | JNull => None
        case _ => None
      },
      rejectionReason = actionConfig match {
        case jobj: JObject => {
          JsonUtil.extractOption[String](jobj \ "reason")
        }
        case JNothing | JNull => None
        case _ => None
      }
    )
  }

  /** Perform the rejection
    *
    */
  def perform(
    sourceFields: List[String], 
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    PerformResult = {

    implicit val jsonFormats = org.json4s.DefaultFormats

    // get the rejection reason from the right place
    val reason = ValidString(
      if (reasonFrom.nonEmpty) context.scratchPad.getResult(reasonFrom.get)
      else if (rejectionReason.nonEmpty) rejectionReason
      else None
    )

    // make sure nonempty, 
    if (reason.nonEmpty)
      context.rejectionReasons.add(reason.get)

    PerformResult()
  }
  
}

