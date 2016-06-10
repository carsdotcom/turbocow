package com.cars.ingestionframework.actions

import com.cars.ingestionframework.Action
import com.cars.ingestionframework.ActionContext
import com.cars.ingestionframework.JsonUtil
import org.json4s._


class SimpleCopy(actionConfig : JValue) extends Action with Serializable
{
  //implicit val jsonFormats = org.json4s.DefaultFormats
  println("^^^^^^^^^^^^^^^^^^^^^^^^^^^"+(actionConfig))

  val newName = JsonUtil.extractOption[String](actionConfig \ "newName")
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

    // for each sourceField, get the data out of the inputRecord, and add it to map to return.
    sourceFields.flatMap{ field =>

      // search in the source json for this field name.
      val found = inputRecord \ field

      if(found == JNothing) {
        // Returning None in a flatMap adds nothing to the resulting collection:
        None
      }
      else {
        // Add this tuple to the resulting list (which is converted to a map later)
        if(newName.isDefined && sourceFields.length > 1) {
            throw new Exception("The config for '"+field+"' require only one source field. Recevied: "+sourceFields.length)
          }
        else if(newName.isDefined && sourceFields.length == 1) {
          Some((newName.get, found.extract[String]))
        }
        else{
          Some((field, found.extract[String]))
          }
        }

    }.toMap
  }
  
}

