package com.cars.turbocow

import org.json4s._

//class RemoveEnrichedFields(actionConfig: JValue) extends Action
//{
//  implicit val jsonFormats = org.json4s.DefaultFormats
//
//  // get list of fields to remove
//  val fieldsToRemove = (actionConfig \ "fields").children.map{ _.extract[String] ) }
//
//  /** Perform the action
//    */
//  def perform(
//    sourceFields: List[String], 
//    inputRecord: JValue, 
//    currentEnrichedMap: Map[String, String],
//    context: ActionContext): 
//    Map[String, String] = {
//
//    // Add the kvList to the enriched data
//    fieldsToRemove.map{ field => 
//      (field, None)
//    }.toMap
//  }
//}



