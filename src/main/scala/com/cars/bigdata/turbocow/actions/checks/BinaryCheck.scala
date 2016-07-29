package com.cars.bigdata.turbocow.actions.checks

import com.cars.bigdata.turbocow.actions.ActionList
import com.cars.bigdata.turbocow.{ActionContext, ActionFactory, JsonUtil, PerformResult, _}
import org.json4s._

class BinaryCheck(
  val left: String,
  val right: String,
  val checker: Checker,
  val fieldSource: Option[FieldSource.Value] = None,
  override val onPass: ActionList = new ActionList,
  override val onFail: ActionList = new ActionList
) extends CheckAction(onPass, onFail) {

  ValidString(left).getOrElse(throw new Exception("""'field/left' was nonexistent or empty ("")"""))
  ValidString(right).getOrElse(throw new Exception("""'right' was nonexistent or empty ("")"""))

  if(checker==null) throw new Exception("Hey buddy, we need a Checker object")

  /** JSON constructor 
    */
  def this(config: JValue, actionFactory: Option[ActionFactory]) = {
    this(
      JsonUtil.extractOptionString(config \ "field").getOrElse(
        JsonUtil.extractOptionString(config \ "left").getOrElse(
          throw new Exception("""JSON configuration for checks are required to have a 'left' or 'field' object"""))),
      JsonUtil.extractOptionString(config \ "right").getOrElse(
          throw new Exception("""JSON configuration for checks are required to have a 'right object""")),

      {
        val operator = JsonUtil.extractValidString(config \ "op").getOrElse(throw new Exception("must specify an operator"))
        operator match {
          case "equals" => new EqualChecker
          case _ => throw new Exception("undefined binary operation  "+operator)
        }
      },
      {
        val sourceOpt = Option(JsonUtil.extractOptionString(config \ "fieldSource").getOrElse(
          JsonUtil.extractOptionString(config \ "leftSource").getOrElse(null)))

        sourceOpt match {
          case None => None
          case Some(source) => source match {
            case "input" => Option(FieldSource.Input)
            case "enriched" => Option(FieldSource.Enriched)
            case s: String => throw new Exception("unrecognized fieldSource / leftSource for unary check action: "+ s)
          }
        }
      },
      new ActionList(config \ "onPass", actionFactory),
      new ActionList(config \ "onFail", actionFactory)
    )
  }

  /** toString.  This isn't great but it's better than nothing.
    */
  override def toString() = {
    
    val sb = new StringBuffer
    sb.append(s"""field = ${left}""")
    sb.append(s"""right = ${right}""")
    sb.append(s""", checker = ${checker.toString}""")
    sb.append(s""", onPass = ${onPass.toString}""")
    sb.append(s""", onFail = ${onFail.toString}""")
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

    // If the check passes, run onPass.
    if (checker.performCheck(
          CheckParams.fromBinaryCheck(this),
          inputRecord, 
          currentEnrichedMap, 
          context)) {

      onPass.perform(inputRecord, currentEnrichedMap, context)
    }
    // otherwise run onFail
    else {
      onFail.perform(inputRecord, currentEnrichedMap, context)
    }
  }

}

