package com.cars.ingestionframework.actions

import com.cars.ingestionframework.Action
import org.json4s._


class SimpleCopy extends Action
{
  /** Simple Copy - simply copies the input(s) to the output.
    *
    */
  def perform(field: List[String], sourceJson: JValue, currentEnrichedMap: Map[String, String]): 
    Map[String, String] = {

    Map.empty[String, String]
  }
  
}

