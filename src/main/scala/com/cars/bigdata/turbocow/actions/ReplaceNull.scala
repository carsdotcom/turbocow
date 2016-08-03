package com.cars.bigdata.turbocow.actions

import com.cars.bigdata.turbocow._
import org.json4s._

import scala.util.Try

/** Main constructor
  *
  * @param fields
  * @param newValue
  * @param outputTo
  */
class ReplaceNull(
  val fields: List[FieldSource],
  val newValue: String,
  val outputTo: FieldLocation.Value = FieldLocation.Enriched) extends Action
{

  // Check the input
  Option(fields).getOrElse(throw new Exception("ReplaceNull: fields value is null"))
  fields.headOption.getOrElse(throw new Exception("ReplaceNull: fields list is empty"))
  ValidString(newValue).getOrElse(throw new Exception("ReplaceNull: newValue was null or empty (\"\")"))
  Option(outputTo).getOrElse(throw new Exception("ReplaceNull: outputTo was null"))

  /** Constructor with a JValue.
    */
  def this(config: JValue) = {

    this(
      fields = {
        (config \ "field").toOption match {
          case Some(f) => List(FieldSource(f))
          case None => (config \ "fields").toOption match {
            case Some(f) => FieldSource.parseList(f)
            case None => throw new Exception("expected either 'field' or 'fields' object in 'replace-null'")
          }
        }
      },
      newValue = JsonUtil.extractValidString(config \ "newValue").getOrElse(throw new Exception("must provide 'newValue' object in a replace-null action")),
      outputTo = {
        val locStr = JsonUtil.extractValidString(config \ "outputTo").getOrElse(throw new Exception("must provide 'outputTo' object in a replace-null action"))
        val loc = Try{ FieldLocation.withName(locStr) }.getOrElse(throw new Exception("Invalid 'outputTo' location: "+locStr))
        if ( !FieldLocation.isOutput(loc) ) throw new Exception("Invalid 'outputTo' location.  Can't write to this location: "+locStr)
        loc
      }
    )
  }

  /** Replace a null value with something else.
    *
    */
  def perform(
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    PerformResult = {

    implicit val jsonFormats = org.json4s.DefaultFormats

    fields.map{ fieldSource => 

      if (fieldSource.isValueNull(inputRecord, currentEnrichedMap, context.scratchPad)) {
        
      }
    }

    PerformResult()
  }
}


