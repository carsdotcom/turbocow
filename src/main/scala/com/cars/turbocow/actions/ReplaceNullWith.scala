package com.cars.turbocow.actions

import com.cars.turbocow.Action
import com.cars.turbocow.ActionContext
import com.cars.turbocow.Defs
import com.cars.turbocow.JsonUtil
import com.cars.turbocow.PerformResult
import org.json4s._


class ReplaceNullWith(replacement: String, sourceList: List[String]) extends Action
{
  /** Construct from a JValue config
    */
  def this(replacement: String, actionConfig: JValue) = {
    this( 
      replacement, 
      actionConfig.toOption match {
        case None => throw new Exception("'config' section is missing in 'replace-null-with' action.")
        case Some(config) => {
          val jSourceList = (actionConfig \ "inputSource" ).children
          val sourceList = jSourceList.flatMap( e=> JsonUtil.extractValidString(e) )
          // check for any empty fields
          if (sourceList.size != jSourceList.size) throw new Exception("'replace-null-with' actions must not have source fields that are null or empty.")
          sourceList
        }
      }
    )
  }

  /** Replace a null value with something else.
    *
    */
  def perform(
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    PerformResult = {

    implicit val jsonFormats = org.json4s.DefaultFormats

    // for each sourceField, get the data out of the inputRecord, and add it to map to return.
    val enrichedUpdates = sourceList.flatMap{ inputFieldName =>
    
      // search in the source json for this field name.
      val inputFieldValue = JsonUtil.extractOptionString(inputRecord \ inputFieldName)

      inputFieldValue match {
        case None => Some( (inputFieldName, replacement) )
        case Some(fieldVal) => None
      }
    }.toMap
    
    PerformResult(enrichedUpdates)
  }
}


