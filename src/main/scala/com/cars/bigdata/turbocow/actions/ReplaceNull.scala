package com.cars.bigdata.turbocow.actions

import com.cars.bigdata.turbocow._
import org.json4s._

import scala.util.Try

import FieldLocation._

/** Main constructor
  *
  * @param fields list of fields in the input record to replace a null value with
  * @param newValue
  * @param outputTo
  */
class ReplaceNull(
  val fields: List[FieldSource],
  val newValue: String,
  val outputTo: FieldLocation.Value = ReplaceNull.defaultLocation) 
  extends Action {

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
          case Some(f) => List(FieldSource.parseJVal(f, Option(ReplaceNull.defaultLocation)))
          case None => (config \ "fields").toOption match {
            case Some(f) => FieldSource.parseJArray(f, Option(ReplaceNull.defaultLocation))
            case None => throw new Exception("expected either 'field' or 'fields' object in 'replace-null'")
          }
        }
      },
      newValue = JsonUtil.extractValidString(config \ "newValue").getOrElse(throw new Exception("must provide 'newValue' object in a replace-null action")),
      outputTo = {
        val errString = "Must be a string, one of 'Enriched', 'Scratchpad'"
        val loc = (config \ "outputTo") match {
          case JNothing => Enriched
          case JNull => throw new Exception("Invalid 'outputTo' location.  Can't write to this location: null.  "+errString)
          case JString(s) => {
            val str = s.toString
            Try{ FieldLocation.withName(str) }.getOrElse(throw new Exception("Invalid 'outputTo' location: "+str+".  "+errString))
          }
          case _ => throw new Exception("Invalid 'outputTo' location.  ")
        }
        if ( !FieldLocation.isOutput(loc) ) throw new Exception("Invalid 'outputTo' location.  Can't write to this location: "+loc+".  "+errString)
        loc
      }
    )
  }

  /** Replace a null value with something else.  If not null, do nothing.
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

        outputTo match {
          case Enriched => PerformResult(Map(fieldSource.name->newValue))
          case Scratchpad => {
            context.scratchPad.set(fieldSource.name, newValue)
            PerformResult()
          }
          case _ => throw new Exception("ReplaceNull: cannot output to anything other than Enriched or Scratchpad!")
        }
      }
      else {
        PerformResult()
      }
    }.reduceLeft{ (combined, e) =>
      combined.combineWith(e)
    } 
  }
}

object ReplaceNull
{
  val defaultLocation = FieldLocation.EnrichedThenInput
}

