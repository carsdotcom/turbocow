package com.cars.bigdata.turbocow.actions.checks

import com.cars.bigdata.turbocow.actions.ActionList
import com.cars.bigdata.turbocow._
import org.json4s._

import FieldLocation._

class BinaryCheck(
  val left: String,
  val right: String,
  val checker: Checker,
  val leftSource: Option[FieldLocation.Value] = None,
  val rightSource: Option[FieldLocation.Value] = None,
  override val onPass: ActionList = new ActionList,
  override val onFail: ActionList = new ActionList
) extends CheckAction(onPass, onFail) {

  ValidString(left).getOrElse(throw new Exception("""'left' was nonexistent or empty ("")"""))
  ValidString(right).getOrElse(throw new Exception("""'right' was nonexistent or empty ("")"""))

  if(checker==null) throw new Exception("Hey buddy, we need a Checker object")

  /** Get the lookup requirements
    */
  override def getLookupRequirements: List[CachedLookupRequirement] = {
    onPass.getLookupRequirements ++ onFail.getLookupRequirements
  }

  /** JSON constructor 
    */
  def this(config: JValue, actionFactory: Option[ActionFactory]) = {
    this(
        JsonUtil.extractOptionString(config \ "left").getOrElse(
          throw new Exception("""JSON configuration for checks are required to have a 'left' object""")),
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
        val sourceOpt = Option(JsonUtil.extractOptionString(config \ "leftSource").getOrElse(null))

        sourceOpt match {
          case None => None
          case Some(source) => source match {
            case "input" => Option(Input)
            case "enriched" => Option(Enriched)
            case "constant" => Option(Constant)
            case s: String => throw new Exception("unrecognized leftSource for binary check action: "+ s)
          }
        }
      },
      {
        val sourceOpt = Option(JsonUtil.extractOptionString(config \ "rightSource").getOrElse(null))

        sourceOpt match {
          case None => None
          case Some(source) => source match {
            case "input" => Option(Input)
            case "enriched" => Option(Enriched)
            case "constant" => Option(Constant)
            case s: String => throw new Exception("unrecognized leftSource for binary check action: "+ s)
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

