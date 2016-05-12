package com.cars.ingestionframework.actions

import com.cars.ingestionframework.Action
import org.json4s._


class ReplaceNullWith(replacement: Int) extends Action
{
  /** Simple Copy - simply copies the input(s) to the output.
    *
    */
  def perform(sourceFields: List[String], sourceJson: JValue, currentEnrichedMap: Map[String, String]): 
    Map[String, String] = {

    Map.empty[String, String]
  }
  
}


