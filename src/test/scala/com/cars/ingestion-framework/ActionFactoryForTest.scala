package com.cars.ingestionframework

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/** ActionFactoryMock - enables creation of test actions
  */
class ActionFactoryForTest extends ActionFactory {

  override def createActionForType(actionType: String, actionConfig: JValue): Action = {

    try {
      super.createActionForType(actionType, actionConfig)
    }
    catch {
      case e: Exception => actionType match {
        case "Action1" => new Action1
        case _ => throw new RuntimeException("action not found in ActionFactoryForTest: "+actionType)
      }
    }
  }
}


