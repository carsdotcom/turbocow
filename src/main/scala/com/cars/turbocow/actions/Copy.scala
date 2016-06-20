package com.cars.turbocow.actions

import com.cars.turbocow.{Action, ActionContext, JsonUtil}
import com.cars.turbocow.PerformResult
import org.json4s.JValue
import org.json4s.JsonAST.JNothing

/**
  * Created by nchaturvedula on 6/10/2016.
  * This class copies an input field into enriched
  * by updating the keyname to @newName value mentioned
  * in the configuration JSON
  */
class Copy(actionConfig : JValue, sourceFields: List[String]) extends Action
{

  val newName = JsonUtil.extractOption[String](actionConfig \ "newName")
  newName.getOrElse{ throw new Exception("For 'copy' actions, the 'newName' field is required in the action's 'config' object.") }

  if (sourceFields.length > 1) 
    throw new Exception("The 'copy' action type does not allow multiple source fields. Please use one 'copy' action per each source field.")

  /** Copy - copies the input(s) to the output by the updating with @newName.
    *
    */
  def perform(
    sourceFields: List[String],
    inputRecord: JValue,
    currentEnrichedMap: Map[String, String],
    context: ActionContext):
    PerformResult = {

    implicit val jsonFormats = org.json4s.DefaultFormats

    val enriched = {

      // for one sourceField, get the data out of the inputRecord, and add it to map to return.
      val sourceField = sourceFields.head

      // search in the source json for this field name.
      val found = inputRecord \ sourceField

      if (found == JNothing) {
        // Returning None in a flatMap adds nothing to the resulting collection:
        None
      }
      else {
        Some((newName.get, found.extract[String]))
      }
    }.toMap

    PerformResult(enriched)
  }
}
