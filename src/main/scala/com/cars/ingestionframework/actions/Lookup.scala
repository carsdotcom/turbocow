package com.cars.ingestionframework.actions

import com.cars.ingestionframework.Action
import com.cars.ingestionframework.ActionContext
import org.apache.spark.sql.hive.HiveContext
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.JsonAST.JNothing
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.DataFrame

import scala.io.Source

class Lookup(actionConfig: JValue) extends Action
{
  def extractString(jvalue : JValue): String = {
    implicit val jsonFormats = org.json4s.DefaultFormats
    jvalue.extract[String]
  }

  val lookupFile = (actionConfig \ "lookupFile").toOption
  val lookupDB = (actionConfig \ "lookupDB").toOption
  val lookupTable = (actionConfig \ "lookupTable").toOption
  val lookupField = extractString(actionConfig \ "lookupField")
  val fieldsToSelect: List[String] =
    (actionConfig \ "fieldsToSelect").children.map{e => extractString(e) }
  val fields = if(fieldsToSelect.length > 1) {
    val tableName = lookupTable match {
      case None => ""
      case some => "`" + extractString(some.get) + "."
    }
    tableName + fieldsToSelect.mkString("`," + tableName)
  }
  else if(fieldsToSelect.length == 1){
    fieldsToSelect(0)
  }
  else ""

  /** Perform the lookup
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
        //case None => { // do hdfs lookup
        //
        //  val lookupVal = extractString(inputRecord \ sourceFields.head)
        //
        //  lookupDB.getOrElse{ throw new Exception("TODO - reject this because lookupDB not found in config") }
        //  val lookupDBStr = lookupDB.get.extract[String]
        //
        //  lookupTable.getOrElse{ throw new Exception("TODO - reject this because lookupTable not found in config") }
        //  val lookupTableStr = lookupTable.get.extract[String]
        //
        //  //val fieldValues = lookupDF.get.select(fields).rdd.map(eachRow => eachRow(0).asInstanceOf[String]).collect()
        //
        //  fieldsToSelect.zipWithIndex.map(tuple => (tuple._1, fieldValues(tuple._2)))
        //  //TODO - change status accepted or rejected
        //}
        case _ => { // local file lookup
        
          // look up local file and parse as json.
          val configAST = parse(Source.fromFile(lookupFile.get.extract[String]).getLines.mkString)

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

