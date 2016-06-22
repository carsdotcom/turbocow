package com.cars.turbocow.actions

import com.cars.turbocow.Action
import com.cars.turbocow.ActionContext
import com.cars.turbocow.JsonUtil
import com.cars.turbocow.PerformResult
import org.json4s._
import com.cars.turbocow.Defs

class SimpleCopy(sourceList: List[String]) extends Action with Serializable
{
  if (sourceList.isEmpty) throw new Exception("'simple-copy' must have at least one 'source' field listed in the config section.")

  /** Constructor with JValue (config) param.
    */
  def this(actionConfig: JValue) = {
    this(
      {
        val jSourceList = (actionConfig \ "inputSource" ).children
        val sourceList = jSourceList.flatMap( e=> JsonUtil.extractValidString(e) )
        // check for any empty fields
        if (sourceList.size != jSourceList.size) throw new Exception("'simple-copy' actions must not have source fields that are null or empty.")
        sourceList
      }
    )
  }

  /** Simple Copy - simply copies the input(s) to the output.
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
        // Returning None in a flatMap adds nothing to the resulting collection:
        case None => None
        case Some(fieldVal) => Some( (inputFieldName, fieldVal) )
      }
    }.toMap
    
    PerformResult(enrichedUpdates)
  }
  
}

