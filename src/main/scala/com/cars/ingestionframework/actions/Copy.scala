package com.cars.ingestionframework.actions

import com.cars.ingestionframework.{Action, ActionContext, JsonUtil}
import org.json4s.JValue
import org.json4s.JsonAST.JNothing

/**
  * Created by nchaturvedula on 6/10/2016.
  */
class Copy(actionConfig : JValue) extends Action {

  val newName = JsonUtil.extractOption[String](actionConfig \ "newName")

  /** Copy - copies the input(s) to the output by the updating with @newName.
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
    sourceFields.flatMap { field =>

      // search in the source json for this field name.
      val found = inputRecord \ field

      if (found == JNothing) {
        // Returning None in a flatMap adds nothing to the resulting collection:
        None
      }
      else {
        if(newName.isEmpty) {
            throw new Exception("The 'newName' in 'copy' config for '"+field+"' should be defined and not null in JSON Configuration")
          }
        else if(newName.isDefined && sourceFields.length > 1) {
          throw new Exception("The 'copy' config for '"+field+"' require only one source field. Received: "+sourceFields.length)
        }
        else {
          Some((newName.get, found.extract[String]))
        }

      }
    }
  }.toMap
}
