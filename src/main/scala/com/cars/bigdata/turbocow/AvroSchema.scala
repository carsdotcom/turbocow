package com.cars.bigdata.turbocow

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.{read, write}

import JsonUtil._

case class AvroSchema(
  `type`: String,
  fields: List[AvroSchemaField],
  namespace: String = "",
  name: String = "",
  doc: String = ""
)
{
  /** Convert this to json.
    */
  def toJson: String = {
    implicit val jsonFormats = org.json4s.DefaultFormats
    pretty(render(parse(write(this))))
  }
}

case class AvroSchemaField(
  name: String, 
  `type`: List[String],
  default: JValue,
  doc: String = ""
)

object AvroSchema
{
  /** Transform a string into an AvroSchema.  Not using parsing due to the 
    * 'default' field, which can't be parsed from an Any.
    */
  def apply(json: String): AvroSchema = {
    val ast = parse(json)
    AvroSchema(
      `type` = extractString(ast \ "type"),
      namespace = extractString(ast \ "namespace"),
      name = extractString(ast \ "name"),
      doc = extractOptionString(ast \ "doc").getOrElse(""),
      fields = (ast \ "fields").children.map{ jvalue => 
        AvroSchemaField(
          name = extractString(jvalue \ "name"),
          `type` = (jvalue \ "type").children.map{ jv => jv.values.toString },
          default = (jvalue \ "default"),
          doc = extractOptionString(jvalue \ "doc").getOrElse("")
        )
      }
    )
  }
}

