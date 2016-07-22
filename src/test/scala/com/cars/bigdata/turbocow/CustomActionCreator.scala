package com.cars.bigdata.turbocow

import com.cars.bigdata.turbocow.actions.AddScratchToEnriched
import org.json4s.JValue

/** ActionFactoryMock - enables creation of test actions
  */
class CustomActionCreator extends ActionCreator {

  override def createAction(
    actionType: String,
    actionConfig: JValue):
    Option[Action] = {

    actionType match {
      case "custom-1" => Option(new Custom1)
      case "custom-add-enriched-fields" => Option(new AddEnrichedFields(actionConfig))
      //case "custom-remove-enriched-fields" => Option(new RemoveEnrichedField(actionConfig))
      case "add-scratch-to-enriched" => Option(new AddScratchToEnriched)
      case _ => None
    }
  }

}



