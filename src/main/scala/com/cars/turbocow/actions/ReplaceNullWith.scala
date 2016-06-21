package com.cars.turbocow.actions

import com.cars.turbocow.Action
import com.cars.turbocow.ActionContext
import com.cars.turbocow.PerformResult
import org.json4s._


class ReplaceNullWith(replacement: Int, sourceFields: List[String]) extends Action
{

  /** Replace a null value with something else.
    *
    */
  def perform(
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    PerformResult = {

    implicit val jsonFormats = org.json4s.DefaultFormats

    // for each sourceField, get the data out of the inputRecord, and see if we need to replace it with a value
    PerformResult( 
      sourceFields.flatMap{ field => 

        // search in the source json for this field name.
        val found = (inputRecord \ field)

        // TODO these could be separated out into separate actions, but this is fine:
        if(found == JNothing || found == JNull) {
          // Add this as the value specified:
          Some((field, replacement.toString))
        }
        else {
          // Otherwise, just do a copy.
          Some((field, found.extract[String]))
        }
        
      }.toMap
    )
  }
}


