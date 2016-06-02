package com.cars.ingestionframework

/** Class holding anything that needs to be broadcast to be used by actions
  * or inside other RDD functions.
  * 
  */
case class ActionContext(
  tableCaches: Map[String, TableCache]
)

