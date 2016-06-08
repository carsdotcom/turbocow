package com.cars.ingestionframework

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.io.Source


/** Util class to work around some json4s/spark limitations.  (can't serialize
  * DefaultFormats, so it has to be used locally every time inside RDD function).
  * @todo do that magic to get these methods tacked onto JValue (see book)
  */      
object JsonUtil {

  def extract[TYPE](jvalue : JValue)(implicit m: Manifest[TYPE]): TYPE = {
    implicit val jsonFormats = org.json4s.DefaultFormats
    jvalue.extract[TYPE]
  }

  def extractOption[TYPE](jvalue: JValue)(implicit m: Manifest[TYPE]): Option[TYPE] = {
    jvalue.toOption match { 
      case None => None
      case some => Option(extract[TYPE](some.get))
    }
  }

  def extractString(jvalue : JValue): String = extract[String](jvalue)
  def extractLong(jvalue : JValue): Long = extract[Long](jvalue)

}

