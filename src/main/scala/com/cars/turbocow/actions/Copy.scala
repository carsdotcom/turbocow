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
            val inputSource = JsonUtil.extractValidString(confElement \ "inputSource")
            inputSource.getOrElse(throw new Exception("'copy' config has empty or missing 'inputSource'.  You must supply a 'inputSource' for each element in a 'copy' config array."))

            val outputTarget = JsonUtil.extractValidString(confElement \ "outputTarget")
            outputTarget.getOrElse(throw new Exception("'copy' config has empty or missing 'outputTarget'.  You must supply a 'outputTarget' for each element in a 'copy' config array."))

            CopyConfigElement(inputSource.get, outputTarget.get)
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
      val inputFieldValue = JsonUtil.extractOptionString(inputRecord \ copyConfig.inputSource)
    
      inputFieldValue match {
        case None => None
        case Some(fieldVal) => Some( (copyConfig.outputTarget, fieldVal) )
      }

    }.toMap

    PerformResult(enriched)
  }
}

case class CopyConfigElement(
  inputSource: String,
  outputTarget: String
)
