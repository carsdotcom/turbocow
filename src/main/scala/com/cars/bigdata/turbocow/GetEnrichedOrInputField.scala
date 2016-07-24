package com.cars.bigdata.turbocow

import org.json4s._

/**
  * Created by nchaturvedula on 7/7/2016.
  *
  * Helper Object to globalize reading of a key's value from enriched Map or
  * from input json Record as secondary Option
  */
object GetEnrichedOrInputField {

  /**
    *  get a key's value from raw input or from enriched map if it already available
    * @param fieldName is extracted from Json Config, for which config is applied
    * @param inputRecord is each Input Json Record
    * @param currentEnrichedMap is Enriched Map that collects all enriched key/values for avro
    * @return
    */
  def apply(
    fieldName : String,
    inputRecord : JValue,
    currentEnrichedMap: Map[String, String]):
    Option[String] ={

    if(currentEnrichedMap.contains(fieldName)) {
      currentEnrichedMap.get(fieldName) // found in enriched map and is enriched. return it
    }
    else {
      JsonUtil.extractOption[String](inputRecord \ fieldName) // did not find in enriched. get from input
    }
  }
}