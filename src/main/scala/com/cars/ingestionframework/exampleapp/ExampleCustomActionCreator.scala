package com.cars.ingestionframework.exampleapp

import org.apache.spark.sql.hive.HiveContext
import org.json4s.JValue

/** Example 
  */
class ExampleCustomActionCreator extends ActionCreator {

  override def createAction(
    actionType: String,
    actionConfig: JValue,
    hiveContext: Option[HiveContext]): 
    Option[Action] = {

    actionType match {
      case "custom-add-enriched-fields" => Option(new AddEnrichedFields(actionConfig))
      case _ => None
    }
  }
}




