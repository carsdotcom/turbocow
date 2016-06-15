package com.cars.turbocow.actions

import com.cars.turbocow.Action
import com.cars.turbocow.ActionContext
import com.cars.turbocow.JsonUtil
import com.cars.turbocow.PerformResult
import org.json4s._


class SimpleCopy extends Action with Serializable
{

  /** Simple Copy - simply copies the input(s) to the output.
    *
    */
  def perform(
    sourceFields: List[String], 
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    PerformResult = {

    implicit val jsonFormats = org.json4s.DefaultFormats

    // for each sourceField, get the data out of the inputRecord, and add it to map to return.
    val enrichedUpdates = sourceFields.flatMap{ field =>

      // search in the source json for this field name.
      val found = inputRecord \ field

      if(found == JNothing) {
        // Returning None in a flatMap adds nothing to the resulting collection:
        None
      }
      else {
        // Add this tuple to the resulting list (which is converted to a map later)
        Some((field, found.extract[String]))
      }
    }.toMap

    PerformResult(enrichedUpdates)
  }
  
}

