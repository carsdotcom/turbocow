package com.cars.ingestionframework.actions

import com.cars.ingestionframework.Action
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.io.Source

class Lookup(actionConfig: JValue) extends Action
{

  // parse the input config:
  implicit val jsonFormats = org.json4s.DefaultFormats

  val lookupFilePath = (actionConfig \ "lookupFilePath").extract[String]
  val keyField = (actionConfig \ "keyField").extract[String]
  val fieldsToSelect: List[String] = 
    (actionConfig \ "fieldsToSelect").children.map{ _.extract[String] }

  /** Simple Copy - simply copies the input(s) to the output.
    *
    */
  def perform(sourceFields: List[String], inputRecord: JValue, currentEnrichedMap: Map[String, String]): 
    Map[String, String] = {

    // The source field must have only one item in it. 
    if(sourceFields.size != 1) return Map.empty[String, String] 
    // TODO - should error out if more than one field in source

    sourceFields.flatMap{ field => 

      // search in the table for this key
      val RE = """hdfs://.*""".r
      lookupFilePath match {
        case RE() => { // do hdfs lookup 

          List.empty[ Tuple2[String, String] ] // TODO - real implementation against the actual file
        }
        case _ => {
        
          // look up local file and parse as json.
          val configAST = parse(Source.fromFile(lookupFilePath).getLines.mkString)

          // get value of source field from the input JSON:
          val lookupKeyVal: String = (inputRecord \ sourceFields.head).extract[String]
          val dimRecord: Option[JValue] = 
            configAST.children.find( record => (record \ keyField) == JString(lookupKeyVal) )
          if (dimRecord.isEmpty) {
            throw new Exception(s"couldn't find dimension record in local file($lookupFilePath) for field($keyField) and lookupKeyVal($lookupKeyVal)")
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

