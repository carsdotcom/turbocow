package com.cars.turbocow

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.io.Source


/** Util class to work around some json4s/spark limitations.  (can't serialize
  * DefaultFormats, so it has to be used locally every time inside RDD function).
  * @todo do that magic to get these methods tacked onto JValue (see book)
  */      
object JsonUtil {

  /** Extract a specific type from a JValue.  
    * 
    */
  def extract[TYPE](jvalue : JValue)(implicit m: Manifest[TYPE]): TYPE = {
    implicit val jsonFormats = org.json4s.DefaultFormats
    jvalue.extract[TYPE]
  }

  /** Return an Option as a result of an extraction.
    * If a JValue is JNothing (or JNull), returns None.
    * If the JValue is (any other JValue), extracts the TYPE and 
    * returns a Some.
    */
  def extractOption[TYPE](jvalue: JValue)(implicit m: Manifest[TYPE]): Option[TYPE] = {
    jvalue.toOption match { 
      case None => None
      case Some(e) => Option(extract[TYPE](e))
    }
  }

  /** Aliases for some common extraction types.
    */
  def extractOptionLong(jvalue : JValue): Option[Long] = this.extractOption[Long](jvalue)
  def extractOptionString(jvalue : JValue): Option[String] = this.extractOption[String](jvalue)

  /** Extract a boolean from a JValue.  If it is JNothing/JNull, you must provide
    * a default value.  If you do not provide a default value, and the value is 
    * empty or not a bool, this throws.
    */
  def extractOptionalBool(jvalue: JValue, default: Boolean): Boolean = {
    jvalue.toOption match { 
      case None => default
      case some => { 
        val boolOpt = extractOption[Boolean](some.get)
        boolOpt.getOrElse(default)
      }
    }
  }

  /** Aliases for some common extraction types.
    */
  def extractLong(jvalue : JValue): Long = this.extract[Long](jvalue)
  def extractString(jvalue : JValue): String = this.extract[String](jvalue)

  /** Extracts a string.  Calls extractOption[String], but does another check
    * to determine if the string is empty or not.  Return None if the string is
    * empty, Some if nonempty.
    */
  def extractValidString(jvalue : JValue): Option[String] = {
    ValidString(this.extractOption[String](jvalue))
  }

}

