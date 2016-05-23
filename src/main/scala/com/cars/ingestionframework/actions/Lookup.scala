package com.cars.ingestionframework.actions

import com.cars.ingestionframework.Action
import com.cars.ingestionframework.ActionContext
import org.apache.spark.sql.hive.HiveContext
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.io.Source

class Lookup(actionConfig: JValue) extends Action
{
  def extractString(jvalue : JValue): String = {
    implicit val jsonFormats = org.json4s.DefaultFormats
    jvalue.extract[String]
  }

  val lookupFile = actionConfig \ "lookupFile"
  val lookupDB = actionConfig \ "lookupDB"
  val lookupTable = actionConfig \ "lookupTable"
  val lookupField = extractString(actionConfig \ "lookupField")
  val fieldsToSelect: List[String] =
    (actionConfig \ "fieldsToSelect").children.map{e => extractString(e) }
  var fields = ""

  if(fieldsToSelect.length > 1) {
    fields = fieldsToSelect.mkString(",")
  }
  else if(fieldsToSelect.length == 1){
    fields = fieldsToSelect(0)
  }
  /** Simple Copy - simply copies the input(s) to the output.
    *
    */
  def perform(
    sourceFields: List[String], 
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    Map[String, String] = {

    implicit val jsonFormats = org.json4s.DefaultFormats

    // The source field must have only one item in it.
    if(sourceFields.size != 1) return Map.empty[String, String] 
    // TODO - should error out if more than one field in source

    sourceFields.flatMap{ field => 

      // search in the table for this key
      lookupFile match {
        case JNothing => { // do hdfs lookup

          val hc = context.hc.getOrElse(throw new Exception("A Hive Context is Required If doing lookup in HDFS"))

          val dataFrame = hc.sql("FROM "+lookupDB.extract[String]+"."+lookupTable.extract[String]+" SELECT "+fields)
            .where(lookupField+"="+(inputRecord \ sourceFields.head).extract[String])

          val fieldValues = dataFrame.select(fields).rdd.map(eachRow => eachRow(0).asInstanceOf[String]).collect()

          fieldsToSelect.zipWithIndex.map(tuple => (tuple._1, fieldValues(tuple._2)))
          //TODO - change status accepted or rejected
        }
        case _ => {
        
          // look up local file and parse as json.
          val configAST = parse(Source.fromFile(lookupFile.extract[String]).getLines.mkString)

          // get value of source field from the input JSON:
          val lookupKeyVal: String = (inputRecord \ sourceFields.head).extract[String]
          val dimRecord: Option[JValue] = 
            configAST.children.find( record => (record \ lookupField) == JString(lookupKeyVal) )
          if (dimRecord.isEmpty) {
            throw new Exception(s"couldn't find dimension record in local file($lookupTable) for field($lookupField) and lookupKeyVal($lookupKeyVal)")
          }
          else { // ok, found it
            fieldsToSelect.map{ selectField => 
              val fieldVal = (dimRecord.get \ selectField).extract[String]
              (selectField, fieldVal)
            }
          }
        }
      }

    }.toMap
  }
  
}

