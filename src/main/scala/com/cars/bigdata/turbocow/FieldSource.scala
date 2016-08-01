package com.cars.bigdata.turbocow

import org.json4s.{JArray, JString, JValue}
import FieldLocation._

import scala.util.Try

// Enum that describes all the possible places to read a field or write a field:
case class FieldSource(
  // the field's name
  name: String,
  // Where to pull it from.  
  source: FieldLocation.Value = EnrichedThenInput
)

object FieldSource {

  /** Parse an (expected) string value into a FieldSource of the form:
    * 
    * "$input.fieldName" = Input
    * "$enriched.fieldName" = Enriched
    * "$scratchpad.fieldName" = Scratchpad
    * "fieldName" = EnrichedThenInput
    */
  def apply(jval: JValue): FieldSource = {
    val wholeStr = JsonUtil.extractValidString(jval).getOrElse(throw new Exception("could not find a valid value for a field name"))
    val split = wholeStr.split('.')
    if (split.size == 1) {
      val field = split.head.trim
      if (field.head == '$') throw new Exception("field - location specified with $ but no field name specified!")
      FieldSource(field)
    }
    else if (split.size == 2) {
      val location: FieldLocation.Value = { 
        val str = split.head.trim
        if (str.head != '$') throw new Exception("field-location must start with '$'.")
        Try( FieldLocation.withName(str.tail) ).getOrElse{
          val validValues = FieldLocation.values.map( _.toString ).toList.mkString(", ")
          throw new Exception(s"field-location must be one of [$validValues].  Was: '$str'")
        }
      }
      val fieldName = split.last.trim
      FieldSource(fieldName, location)
    }
    else throw new Exception("Can only have one '.' in the field name.")
  }

  /** Parse a list of FieldSources.  Expecting a parsed JArray of the form
    * [ "$location.fieldname", "$location2.fieldname2" ] 
    */
  def parseList(jval: JValue): List[FieldSource] = {
    jval match {
      case jarray: JArray => {
        jarray.children.map{ str => FieldSource(str) }
      }
      case _ => throw new Exception("expected an array.")
    }
  }
}
