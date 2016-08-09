package com.cars.bigdata.turbocow.actions.checks

import com.cars.bigdata.turbocow.{FieldLocation, JsonUtil}
import org.json4s.JValue

case class CheckParams(
  val left: String,
  val right: Option[String] = None,
  val leftSource: Option[FieldLocation.Value] = None,
  val rightSource: Option[FieldLocation.Value] = None
) extends Serializable {

  /** Get the value from either input record or enriched map.
    */
  def getValueFrom(
    field: Option[String],
    fieldSource: Option[FieldLocation.Value],
    inputRecord: JValue,
    currentEnrichedMap: Map[String, String]
  ): Option[String] = {

    // todo consolidate this code with FieldSource.  Maybe use FieldSource instead of left+leftSource pattern.
    val valueOpt = field match {
      case None => None
      case Some(f) => {
        fieldSource match {
          case None => { 
            val enrichedOpt = currentEnrichedMap.get(f)
            if (enrichedOpt.nonEmpty) enrichedOpt
            else JsonUtil.extractOptionString(inputRecord \ f)
          }
          case Some(FieldLocation.Input) => JsonUtil.extractOptionString(inputRecord \ f)
          case Some(FieldLocation.Enriched) => currentEnrichedMap.get(f)
          case Some(FieldLocation.Constant) => field
          case a: Any => throw new Exception("unrecognized field source:"+ a.toString)
        }
      }
    }

    // Special case:  enriched record could potentially have an actual null as the value.
    // Check and convert to None.
    if (valueOpt.nonEmpty && valueOpt.get == null ) None
    else valueOpt
  }

  /** Get the left value from the correct place, or None if not exists.
    */
  def getLeftValue(
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String]): 
    Option[String] = 
    getValueFrom(Option(left), leftSource, inputRecord, currentEnrichedMap)

  /** Get the right value from the correct place, or None if not exists.
    */
  def getRightValue(
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String]): 
    Option[String] = 
    getValueFrom(right, leftSource, inputRecord, currentEnrichedMap)
}

object CheckParams {

  def fromUnaryCheck(uc: UnaryCheck): CheckParams = CheckParams(
    uc.field
  )


  def fromBinaryCheck(bc: BinaryCheck): CheckParams = CheckParams(
    bc.left,
    Option(bc.right),
    bc.leftSource,
    bc.rightSource
  )
}




