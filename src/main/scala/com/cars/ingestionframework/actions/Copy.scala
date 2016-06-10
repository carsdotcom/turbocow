package com.cars.ingestionframework.actions

import com.cars.ingestionframework.{Action, ActionContext, JsonUtil}
import org.json4s.JValue
import org.json4s.JsonAST.JNothing

/**
  * Created by nchaturvedula on 6/10/2016.
  * This class copies an input field into enriched
  * by updating the keyname to @newName value mentioned
  * in the configuration JSON
  */
class Copy(actionConfig : JValue) extends Action
{

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

    if(sourceFields.length == 1) {

      // for one sourceField, get the data out of the inputRecord, and add it to map to return.
      val field = sourceFields.head

      // search in the source json for this field name.
      val found = inputRecord \ field

      if (found == JNothing) {
        // Returning None in a flatMap adds nothing to the resulting collection:
        None
      }
      else {
        if(newName.isEmpty) {
          throw new Exception("For 'copy' actions, the 'newName' field is required in the action's 'config' object.")
        }
        else {
          Some((newName.get, found.extract[String]))
        }
      }
    }
    else{
      throw new Exception("The 'copy' action type does not allow multiple source fields. Please use one 'copy' action per each source field.")
    }

  }.toMap
}
