package com.cars.bigdata.turbocow

import org.json4s.{JArray, JString, JValue}
import FieldLocation._
import org.json4s.JsonAST.JNull

import scala.util.Try

case class FieldSource(
  // the field's name
  name: String,
  // Where to pull it from.  
  source: FieldLocation.Value = EnrichedThenInput
)
{
  
  /** Convenience method to get a string value from either input, enriched map, 
    * or scratchpad, depending on the source in this instance.
    * 
    * Note the return type is Option[String].  It cannot differentiate between
    * a value that is actually null or just plain missing in this return type,
    * since they all return None.  If your logic requires differentiating between
    * the two, you should not use this convenience method and write your own.
    * You may also use isValueNull, below, to check for this case.
    * 
    * Also, since it return a String, if you are getting a scratchpad value, it 
    * will be converted to a String via toString if it is not a string.  If 
    * that is not desired, use your own implementation.
    * 
    */  
  def getValue(
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    scratchPad: ScratchPad,
    constantValue : Option[String]):
    Option[String] = {

    source match {
      case Constant => constantValue match {
        case None => None
        case Some(null) => None
        case null => None
        case _ => constantValue
      }
      case Input => JsonUtil.extractOptionString(inputRecord \ name)
      case Enriched => {
        val enrOpt = currentEnrichedMap.get(name)
        enrOpt match {
          case None => None
          case Some(null) => None // this could happen if the key exists in the map but the val is null
          case Some(s) => enrOpt
        }
      }
      case Scratchpad => {
        scratchPad.get(name) match {
          case None => None
          case Some(null) => None // this could happen if the key exists in the map but the val is null
          case Some(s) => s match {
            case str: String => Option(str)
            case a: Any => Option(a.toString)
          }
        }
      }
      case EnrichedThenInput => {
        val enrichedOpt = currentEnrichedMap.get(name) 
        enrichedOpt match {
          case None => JsonUtil.extractOptionString(inputRecord \ name)
          case _ => enrichedOpt
        }
      }
    }
  }

  /** Check if a value is null based on input type.
    * This returns true only if the value exists in the location, but it is null.
    */
  def isValueNull(
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    scratchPad: ScratchPad,
    constantValue : Option[String]):
    Boolean = {

    source match {
      case Constant => constantValue match {
        case None => true
        case Some(null) => true
        case null => true
        case _ => false
      }
      case Input => (inputRecord \ name) match { 
        case JNull => true
        case _ => false 
      }
      case Enriched => {
        val enrOpt = currentEnrichedMap.get(name)
        enrOpt match {
          case Some(null) => true // this could happen if the key exists in the map but the val is null
          case _ => false
        }
      }
      case Scratchpad => {
        scratchPad.get(name) match {
          case Some(null) => true // this could happen if the key exists in the map but the val is null
          case _ => false
        }
      }
      case EnrichedThenInput => {
        val enrichedOpt = currentEnrichedMap.get(name) 
        enrichedOpt match {
          case None => (inputRecord \ name) match { 
            case JNull => true
            case _ => false 
          }
          case Some(null) => true // this could happen if the key exists in the map but the val is null
          case _ => false
        }
      }
    }
  }
}

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
