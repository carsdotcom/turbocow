package com.cars.ingestionframework.actions

import com.cars.ingestionframework.Action
import com.cars.ingestionframework.ActionContext
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
  def extractLong(jvalue : JValue): Long = {
    implicit val jsonFormats = org.json4s.DefaultFormats
    jvalue.extract[Long]
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
        case None => { // do hdfs "lookup"
        
          val caches = context.tableCaches
          if (caches.isEmpty) {
            // return empty map - can't look up into nonexistent tables!  (todo - reject or throw?)
            Map.empty[String, String]
          }
          else {  // cache is not empty

            lookupDB.getOrElse{ throw new Exception("TODO - reject this because lookupDB not found in config") }
            lookupTable.getOrElse{ throw new Exception("TODO - reject this because lookupTable not found in config") }

            val lookupDBAndTable = s"${extractString(lookupDB.get)}.${extractString(lookupTable.get)}"
            val tableCacheOpt = caches.get(lookupDBAndTable)
            tableCacheOpt.getOrElse{ throw new Exception("couldn't find cached lookup table for: "+lookupDBAndTable) }
                                   
            // get the table cache and do lookup
            val tc = tableCacheOpt.get
            // todo what if not there, todo check the enriched record first
            fieldsToSelect.map{ field => 
              val resultOpt = tc.lookup(
                lookupField, 
                extractString(inputRecord \ sourceFields.head).toLong, // TODOTODO this is forced to a fixed type... how to resolve?
                field)
              if (resultOpt.isEmpty) Map.empty[String, String]
              else Map(field -> resultOpt.get)
            }.reduce( _ ++ _ ) // reduce all maps into one
          }

          //TODO - change status accepted or rejected
        }
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

