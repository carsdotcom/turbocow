package com.cars.turbocow.actions

import com.cars.turbocow.Action
import com.cars.turbocow.ActionContext
import com.cars.turbocow.JsonUtil
import com.cars.turbocow.PerformResult
import org.json4s._
import com.cars.turbocow.Defs

class SimpleCopy(sourceList: List[String]) extends Action with Serializable
{
  if (sourceList.isEmpty) throw new Exception("'simple-copy' must have at least one 'source' field listed.")

  /** Constructor with JValue (config) param.
    */
  def this(actionConfig: JValue) = {
    this(
      {
        val jSourceList = (actionConfig \ "source" ).children
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
    //val enrichedUpdates = sourceFields.flatMap{ field =>
    //
    //  // search in the source json for this field name.
    //  val found = inputRecord \ field
    //
    //  if(found == JNothing) {
    //    // Returning None in a flatMap adds nothing to the resulting collection:
    //    None
    //  }
    //  else {
    //    // Add this tuple to the resulting list (which is converted to a map later)
    //    Some((field, found.extract[String]))
    //  }
    //}.toMap
    //
    //PerformResult(enrichedUpdates)
    PerformResult(Defs.emptyStringMap)
  }
  
}

