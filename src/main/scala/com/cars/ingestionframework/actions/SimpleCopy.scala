package com.cars.ingestionframework.actions

import com.cars.ingestionframework.Action
import org.json4s._


class SimpleCopy extends Action
{
  implicit val jsonFormats = org.json4s.DefaultFormats

  /** Simple Copy - simply copies the input(s) to the output.
    *
    */
  def perform(sourceFields: List[String], sourceJson: JValue, currentEnrichedMap: Map[String, String]): 
    Map[String, String] = {

    // for each sourceField, get the data out of the sourceJson, and add it to map to return.
    println("=============== sourceFields = "+sourceFields)
    sourceFields.flatMap{ field => 

      println("========= field = "+field)

      // search in the source json for this field name.
      val found = (sourceJson \ field)

      if(found == JNothing) {
        println("========= JNOTHING")
        None
      }
      else {
        println("========= returning tuple")
        Some((field, found.extract[String]))
      }
      
    }.toMap
  }
  
}

