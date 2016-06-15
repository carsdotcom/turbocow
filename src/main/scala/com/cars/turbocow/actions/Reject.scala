package com.cars.turbocow.actions

import com.cars.turbocow.Action
import com.cars.turbocow.ActionContext
import com.cars.turbocow.JsonUtil
import com.cars.turbocow.PerformResult
import com.cars.turbocow.RejectionReason
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.JsonAST.JNothing
import org.json4s.jackson.JsonMethods._

import scala.io.Source

class Reject(rejectionReason: Option[RejectionReason])
extends Action {

  /** constructor with a JValue
    * If there is a configuration section for this reject action, it will 
    * override anything passed in the constructor.
    * 
    * @param  actionConfig the parsed configuration for this action
    */
  def this(actionConfig: JValue, rejectionReason: Option[RejectionReason]) = {
    this(
      actionConfig match { 
        case jobj: JObject => {
          // parse it, set RR
          val configReason = JsonUtil.extractOption[String](jobj \ "reason")
          if (configReason.isEmpty && rejectionReason.isEmpty) Option(RejectionReason(""))
          else if (configReason.nonEmpty && rejectionReason.isEmpty) Option(RejectionReason(configReason.get))
          else if (configReason.isEmpty && rejectionReason.nonEmpty) rejectionReason
          else // (if (configReason.nonEmpty && rejectionReason.nonEmpty) )
          { 
            rejectionReason.get.reason = configReason.get
            rejectionReason
          }
        }
        case JNothing | JNull => rejectionReason
        case _ => throw new Exception("The 'config' section for 'reject' must be an object with a 'reason' field.")
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

    val rejectionString = 
      if (rejectionReason.isEmpty) ""
      else rejectionReason.get.reason

    PerformResult(Map("reasonForReject"->rejectionString))
  }
  
}


