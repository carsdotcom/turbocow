package com.cars.bigdata.turbocow

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.{read, write}
import JsonUtil._
import AvroOutputWriter._
import org.apache.spark.sql.types.{NullType, StructField, StructType}

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

  def toListAvroFieldConfig: List[AvroFieldConfig] = {
    fields.map{ _.toAvroFieldConfig }
  }

  def toStructType: StructType = {
    StructType( fields.map{ _.toAvroFieldConfig.structField }.toArray )
  }
}

case class AvroSchemaField(
  name: String, 
  `type`: List[String],
  default: JValue,
  doc: String = ""
)
{
  def toAvroFieldConfig: AvroFieldConfig = {

    AvroFieldConfig(
      this.toStructField,
      default
    )
  }

  def toStructField: StructField = {

    val typeList = `type`.map { t => getDataTypeFromString(t) }
    val nullable = typeList.contains(NullType)

    // Filter out the non-null types.
    // Can only have one non-null data type listed.  We wouldn't know how to handle 
    // a type that can be string OR int.
    val filtered = typeList.filter( _ != NullType )
    val dataType = filtered.size match {
      // No non-nulls.  Only allowed if NullType is the only type (nullable is set)
      case 0 => if (nullable) NullType; else throw new Exception("couldn't determine type for avro field name="+name)
      // One non-null, perfect.
      case 1 => filtered.head
      // cannot have more than one (non-null) data type listed.
      case i: Int => throw new Exception(s"not able to parse type list for avro field: '$name'.  Cannot have more than one non-null data type listed.")
    }

    StructField(name, dataType, nullable)
  }
}

object AvroSchemaField {
  def apply(jvalue: JValue): AvroSchemaField = {
    AvroSchemaField(
      name = ValidString(extractValidString(jvalue \ "name").getOrElse(throw new Exception("must supply a valid 'name' field for all fields in avro schema")).trim)
        .getOrElse(throw new Exception("must specify a valid non-empty string for every field 'name'")),
      `type` = (jvalue \ "type") match {
        case j: JArray => j.children.map{ jv => jv.values.toString }
        case j: JString => List(extractValidString(j).getOrElse(throw new Exception("must supply a valid 'type' field for all fields in avro schema")))
        case a: Any => throw new Exception("the `type` field in the avro schema can only be a string or list of strings")
      },
      default = (jvalue \ "default") match {
        case JNothing => throw new Exception("must specify a 'default' value for every field in the avro schema")
        case j: JValue => j
      },
      doc = extractOptionString(jvalue \ "doc").getOrElse("")
    )
  }
}

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
        AvroSchemaField(jvalue)
      }
    )
  }
}

