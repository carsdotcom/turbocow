package com.cars.turbocow.actions

import com.cars.turbocow.{Action, ActionContext, JsonUtil, ValidString}
import com.cars.turbocow.PerformResult
import org.json4s.JValue
import org.json4s.JsonAST.JNothing

/**
  * Created by nchaturvedula on 6/10/2016.
  * This class copies an input field into enriched
  * by updating the keyname to @newName value mentioned
  * in the configuration JSON
  */
class Copy(actionConfig: JValue) extends Action
{
  throw new Exception("TODO")
  //if (sourceFields.length > 1) 
  //  throw new Exception("The 'copy' action type does not allow multiple source fields. Please use one 'copy' action per each source field.")
  //else if (sourceFields.length != 1) 
  //  throw new Exception("Must specify exactly one source field for the 'copy' action.  Please add a 'source' field.")
  //else 
  //  ValidString(sourceFields.head).getOrElse("The 'source' field must not be blank for the 'copy' action.  Please add a valid source field.")

  val newName = JsonUtil.extractOption[String](actionConfig \ "newName")
  newName.getOrElse{ throw new Exception("For 'copy' actions, the 'newName' field is required in the action's 'config' object.") }

  /** Copy - copies the input(s) to the output by the updating with @newName.
    *
    */
  def perform(
    inputRecord: JValue,
    currentEnrichedMap: Map[String, String],
    context: ActionContext):
    PerformResult = {

    implicit val jsonFormats = org.json4s.DefaultFormats

    val enriched = Map.empty[String, String]

    //val enriched = {
    //
    //  // search in the source json for this field name.
    //  val inputFieldValue = inputRecord \ source
    //
    //  if (inputFieldValue == JNothing) {
    //    // Returning None in a flatMap adds nothing to the resulting collection:
    //    None
    //  }
    //  else {
    //    Some((newName.get, inputFieldValue.extract[String]))
    //  }
    //}.toMap

    PerformResult(enriched)
  }
}

case class CopyConfigElement(
  sourceName: String,
  enrichedName: String
)
