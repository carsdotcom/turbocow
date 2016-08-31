package com.cars.bigdata.turbocow

import org.json4s.JValue
import AvroOutputWriter._
import org.apache.spark.sql.types.StructField

case class AvroFieldConfig(
  structField: StructField,
  defaultValue: JValue
)

object AvroFieldConfig {

  /** Create a new one based on a json JValue.
    */
  def apply(config: JValue): AvroFieldConfig = {
    implicit val jsonFormats = org.json4s.DefaultFormats
    AvroFieldConfig(
      getStructFieldFromAvroElement(config),
      (config \ "default")
    )
  }
}

