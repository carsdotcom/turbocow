package com.cars.turbocow.exampleapp

import org.json4s.JValue

import com.cars.turbocow.ActionCreator
import com.cars.turbocow.Action
import com.cars.turbocow.AddEnrichedFields
import com.cars.turbocow.PerformResult

/** Example 
  */
class ExampleCustomActionCreator extends ActionCreator {

  override def createAction(
    actionType: String, 
    actionConfig: JValue,
    sourceFields: List[String],
    destination: Option[String] ): 
    Option[Action] = {

    actionType match {
      case "custom-add-enriched-fields" => Option(new AddEnrichedFields(actionConfig))
      case _ => None
    }
  }
}




