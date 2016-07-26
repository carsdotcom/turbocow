package com.cars.bigdata.turbocow.actions

import com.cars.bigdata.turbocow.{JsonUtil, _}
import org.json4s._

class AddRejectionReason(field: String) extends Action
{
  import AddRejectionReason._

  ValidString(field).getOrElse(throw new Exception("field must not be null or an empty string"))

  /** Constructor with JValue (config) param.
    */
  def this(actionConfig: JValue) = {
    this(
      JsonUtil.extractValidString(actionConfig \ "field").getOrElse(throw new Exception("'field' member of add-rejection-reason config must be present and be a nonempty string"))
    )
  }

  /** Perform
    *
    */
  def perform(
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    PerformResult = {

    // If we are rejecting this record
    if (context.rejectionReasons.nonEmpty) {
    
      // add any rejection reasons to the enriched record
      val enrichedMap = currentEnrichedMap + (field -> context.rejectionReasons.toString)
    
      // copy in all the fields from the input record.
      PerformResult( addAllFieldsToEnriched(inputRecord, enrichedMap) )
    }
    else {
      // otherwise, no change.  reasonForReject should be default-populated by output writer
      PerformResult()
    }
  }
}

object AddRejectionReason {

  /** Add all fields from input record (as parsed AST) to the enriched map.
    * 
    * @param inputRecordAST the input record as parsed JSON AST
    * @param enrichedMap the current enriched record
    * @return the new enriched map
    */
  def addAllFieldsToEnriched(
    inputRecordAST: JValue, 
    enrichedMap: Map[String, String]): 
    Map[String, String] = {

    val inputMap: Map[String, Option[String]] = inputRecordAST match { 
      case JNothing => Map.empty[String, Option[String]]
      case JObject(o) => o.toMap.map{ case (k,v) => (k, JsonUtil.extractOptionString(v)) }
      case _ => throw new Exception("The input record must be a JSON object (not an array or other type).")
      // TODO double check the ALS code so that it always outputs an object.
    }

    val inputToEnrichedMap = inputMap.flatMap{ case(k, v) => 
      // if key is not in the enriched map, add it.
      val enrichedOpt = enrichedMap.get(k)
      if (enrichedOpt.isEmpty) { // not found
        Some((k, v.getOrElse("")))
      }
      else { // otherwise don't add it
        None
      }
    } 

    // return the merged maps
    inputToEnrichedMap ++ enrichedMap
  }

  
}

