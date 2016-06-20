package com.cars.turbocow

import org.json4s.JValue

/** ActionFactoryMock - enables creation of test actions
  */
class CustomActionCreator extends ActionCreator {

  override def createAction(
    actionType: String, 
    actionConfig: JValue,
    sourceFields: List[String],
    destination: Option[String] ): 
    Option[Action] = {

    actionType match {
      case "custom-1" => Option(new Custom1)
      case "custom-add-enriched-fields" => Option(new AddEnrichedFields(actionConfig))
      //case "custom-remove-enriched-fields" => Option(new RemoveEnrichedField(actionConfig))
      case _ => None
    }
  }

}



