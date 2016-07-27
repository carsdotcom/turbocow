package com.cars.bigdata.turbocow.actions

import com.cars.bigdata.turbocow.{Action, ActionContext, JsonUtil, PerformResult}
import org.json4s._

/**
  * Created by nchaturvedula on 7/11/2016.
  */
class SearchAndReplace(
  searchFor: String,
  replaceWith : String,
  inputSource : List[String])
  extends Action {

  if(inputSource.isEmpty){
    throw new Exception(" 'inputSource' field is missing/invalid in 'search-and-replace' action ")
  }
  /**
    * intialize regex terms and actionConfig from config
    *
    * @param actionConfig
    */
  def this(
    actionConfig: JValue) = {
    this(
      searchFor = JsonUtil.extractValidString( actionConfig \ "searchFor")
        .getOrElse( throw new Exception(" 'searchFor' field is missing/invalid for 'search-and-replace' action")),
      replaceWith = JsonUtil.extractValidString(actionConfig \ "replaceWith")
        .getOrElse( throw new Exception("'replaceWith' field is missing/invalid for 'search-and-repalce' action")),
      inputSource = (actionConfig \ "inputSource").children.map{ eachInput =>
        JsonUtil.extractValidString(eachInput)
          .getOrElse( throw new Exception(" 'inputSource' field is empty/missing/Invalid in 'search-and-replace' action"))
      }
    )
  }
  /**
    * Replaces any string with any string for the input value of a Json Config Field.
    * Returns Empty
    *
    * @param inputRecord
    * @param currentEnrichedMap
    * @param context
    * @return
    */
  def perform(
    inputRecord: JValue,
    currentEnrichedMap: Map[String, String],
    context: ActionContext):
    PerformResult = {

    PerformResult(
      inputSource.map { eachInput =>
        //grab input value from inputRecord
        val inputValue = JsonUtil.extractOption[String](inputRecord \ eachInput)

        if(inputValue.isDefined){
          //use String method to replace anything with anything. can be changed to replaceAll if there is requirement
          val result = inputValue.get.replaceAll(searchFor, replaceWith)

          //return replaced String
          Map(eachInput -> result)
        }
        else {
          // if the inputfield Doesnt exist or null in inputRecord. send null
          Map.empty[String, String]
        }
      }.reduce(_ ++ _)
    )
  }
}
