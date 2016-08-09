package com.cars.bigdata.turbocow.actions.checks

import com.cars.bigdata.turbocow.{ActionContext, ActionFactory, JsonUtil, PerformResult, _}
import com.cars.bigdata.turbocow.actions.ActionList
import org.json4s._

class UnaryCheck(
  val field: String,
  val checker: Checker,
  val fieldSource: Option[FieldLocation.Value] = None,
  override val onPass: ActionList = new ActionList,
  override val onFail: ActionList = new ActionList
) extends CheckAction(onPass, onFail) {

  ValidString(field).getOrElse(throw new Exception("""'field' was nonexistent or empty ("")"""))

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
      JsonUtil.extractOptionString(config \ "field").getOrElse(
        JsonUtil.extractOptionString(config \ "left").getOrElse(
          throw new Exception("""JSON configuration for checks are required to have a 'left' or 'field' object"""))),
      {
        val operator = JsonUtil.extractValidString(config \ "op").getOrElse(throw new Exception("must specify an operator"))
        operator match {
          case "empty" => new EmptyChecker
          case "not-empty" => new InverseChecker(new EmptyChecker)
          case "numeric" => new NumericChecker
          case "non-numeric" => new InverseChecker(new NumericChecker)
          case "true" => new TrueChecker
          case "false" => new InverseChecker(new TrueChecker)
          case _ => throw new Exception("undefined unary operation (was 'right' specified where it is not needed?): "+operator)
        }
      },
      {
        val sourceOpt = Option(JsonUtil.extractOptionString(config \ "fieldSource").getOrElse(
          JsonUtil.extractOptionString(config \ "leftSource").getOrElse(null)))

        sourceOpt match {
          case None => None
          case Some(source) => source match {
            case "input" => Option(FieldLocation.Input)
            case "enriched" => Option(FieldLocation.Enriched)
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
    sb.append(s"""field = ${field}""")
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
          CheckParams.fromUnaryCheck(this), 
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

