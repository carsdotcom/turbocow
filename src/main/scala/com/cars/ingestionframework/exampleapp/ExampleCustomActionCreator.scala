package com.cars.ingestionframework.exampleapp

import org.json4s.JValue

import com.cars.ingestionframework.ActionCreator
import com.cars.ingestionframework.Action
import com.cars.ingestionframework.AddEnrichedFields

/** Example 
  */
class ExampleCustomActionCreator extends ActionCreator {

  override def createAction(
    actionType: String,
    actionConfig: JValue): 
    Option[Action] = {

    actionType match {
      case "custom-add-enriched-fields" => Option(new AddEnrichedFields(actionConfig))
      case _ => None
    }
  }
}




