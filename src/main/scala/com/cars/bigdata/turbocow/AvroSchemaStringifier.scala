package com.cars.bigdata.turbocow

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.{read, write}

import scala.io.Source
import utils.FileUtil

object AvroSchemaStringifier
{
  implicit val jsonFormats = org.json4s.DefaultFormats
  
  /** Convert all of the types inside a schema to strings.  Change the defaults
    * to match.
    */
  def convertToStringTypes(inputAvroSchemaJson: String): String = {
    val inputSchema = AvroSchema(inputAvroSchemaJson)

    val outputSchema: AvroSchema = inputSchema.copy(fields =
      inputSchema.fields.map{ inputField => 
        val newType = inputField.`type`.map{ t => 
          t.trim match {
            case "null" => "null"
            case _ => "string"
          }
        }
        val nullable = newType.contains("null")

        val newDefault = inputField.default match {
          case JNull => {
            if (nullable) JNull
            else throw new RuntimeException(s"field ${inputField.name} has a null default value but is not nullable")
          }
          case v: JString => JString(v.values.toString)
          case v: JInt => JString(v.values.toString)
          case v: JDouble => JString(v.values.toString)
          case v: JBool => JString(v.values.toString)
          //case _ => JString(inputField.default.toString)
        }

        inputField.copy(
          `type` = newType,
          default = newDefault
        )
      }
    )

    // convert back to string and return
    outputSchema.toJson
  }

  /** write an all-string schema out to a file.  The file is always overwritten.
    * 
    */
  def writeStringSchema(inputAvroSchemaFilePath: String, outputFilePath: String): Unit = {

    // read in the file
    val inputStr = Source.fromFile(inputAvroSchemaFilePath).getLines.mkString

    // convert to string
    val outputStr = convertToStringTypes(inputStr)

    // Write out
    FileUtil.writeFile(outputStr, outputFilePath)
  }
}
