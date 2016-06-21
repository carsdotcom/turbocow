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
class Copy(copyConfigList: List[CopyConfigElement]) extends Action
{
  if (copyConfigList.isEmpty) throw new Exception("'copy' config is missing or has zero elements.  Must supply at least one element in the config array.")

  /** Construct from a JValue config
    */
  def this(actionConfig: JValue) = {
    this( 
      actionConfig.toOption match {
        case None => throw new Exception("'config' section is missing in 'copy' action.")
        case Some(config) => {
          config.children.map{ confElement: JValue =>
            val sourceName = JsonUtil.extractValidString(confElement \ "sourceName")
            sourceName.getOrElse("'copy' config has empty or missing 'sourceName'.  You must supply a 'sourceName' for each element in a 'copy' config array.")
            val enrichedName = JsonUtil.extractValidString(confElement \ "enrichedName")
            enrichedName.getOrElse("'copy' config has empty or missing 'enrichedName'.  You must supply a 'enrichedName' for each element in a 'copy' config array.")
            CopyConfigElement(sourceName.get, enrichedName.get)
          }
        }
      }
    )
  }

  /** Copy - copies the input(s) to the output by the updating with @newName.
    *
    */
  def perform(
    inputRecord: JValue,
    currentEnrichedMap: Map[String, String],
    context: ActionContext):
    PerformResult = {

    implicit val jsonFormats = org.json4s.DefaultFormats

    val enriched = copyConfigList.flatMap { copyConfig =>
    
      // search in the source json for this field name.
      val inputFieldValue = JsonUtil.extractOptionString(inputRecord \ copyConfig.sourceName)
    
      inputFieldValue match {
        case None => None
        case Some(fieldVal) => Some( (copyConfig.enrichedName, fieldVal) )
      }

    }.toMap

    PerformResult(enriched)
  }
}

case class CopyConfigElement(
  sourceName: String,
  enrichedName: String
)
