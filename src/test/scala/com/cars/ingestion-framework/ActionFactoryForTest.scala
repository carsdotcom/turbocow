package com.cars.ingestionframework

import org.apache.spark.sql.hive.HiveContext
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/** ActionFactoryForTest - enables creation of test actions Action1 & Action2
  * without using custom creator mechanism.
  */
class ActionFactoryForTest(customActionFactories: List[ActionCreator] = List.empty[ActionCreator] ) 
  extends ActionFactory(customActionFactories)
{

  override def createAction(actionType: String, actionConfig: JValue, hiveContext: Option[HiveContext]): Option[Action] = {

    val action = super.createAction(actionType, actionConfig, hiveContext)

    if (action.isEmpty) actionType match {
      case "Action1" => Option(new Action1)
      case "Action2" => Option(new Action2)
      case _ => None
    }
    else action
  }
}

