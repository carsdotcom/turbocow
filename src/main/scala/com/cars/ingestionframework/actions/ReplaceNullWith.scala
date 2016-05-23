package com.cars.ingestionframework.actions

import com.cars.ingestionframework.Action
import com.cars.ingestionframework.ActionContext
import org.json4s._


class ReplaceNullWith(replacement: Int) extends Action
{

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

    // for each sourceField, get the data out of the inputRecord, and see if we need to replace it with a value
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
  }
  
}


