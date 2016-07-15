package com.cars.bigdata.turbocow.exampleapp

import org.json4s.JValue

import com.cars.bigdata.turbocow.ActionCreator
import com.cars.bigdata.turbocow.Action
import com.cars.bigdata.turbocow.AddEnrichedFields
import com.cars.bigdata.turbocow.PerformResult

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




