package com.cars.bigdata.turbocow

import AvroOutputWriter._
import org.apache.spark.sql.types._
import org.json4s.JsonAST._

case class AvroFieldConfig(
  structField: StructField,
  defaultValue: JValue
) {

  /** Get the default value according to what type it is.
    *  
    * @return 'primitive' type (Int, Float, etc.) in an Any;
    *         throws on JNothing (no default provided) or
    *         the JSON type was not a string, numeric, boolean,
    *         or null.
    */
  def getDefaultValue: Any = {
    implicit val jsonFormats = org.json4s.DefaultFormats
    defaultValue match {
      case j: JString => j.extract[String]
      case j: JInt => structField.dataType match {
        case IntegerType => j.extract[Int]
        case LongType => j.extract[Long]
      }
      case j: JDouble => structField.dataType match {
        case FloatType => j.extract[Float]
        case DoubleType => j.extract[Double]
      }
      case j: JBool => j.extract[Boolean]
      case JNull => null
      case JNothing => throw new Exception("no default value was specified")
      case _ => throw new Exception(s"unsupported JSON type specified as 'default' value for '${structField.name}' field.")
    }
  }


  /** Check for existence of the default value as well as its type.
    * 
    * @throws Exception if default value does not exist, or the 
    *         type is not compatible with the field StructField.
    */
  def checkDefaultValue: Unit = {

    /*
    config.defaultValue match {
      case j: JString => if (structField.)
      case j: JInt => structField.dataType match {
        case IntegerType => j.extract[Int]
        case LongType => j.extract[Long]
      }
      case j: JDouble => structField.dataType match {
        case FloatType => j.extract[Float]
        case DoubleType => j.extract[Double]
      }
      case j: JBool => j.extract[Boolean]
      case JNull => null
      case JNothing => throw new Exception("no default value was specified")
      case _ => throw new Exception(s"unsupported JSON type specified as 'default' value for '${structField.name}' field.")
      
    }
*/
  }

}

object AvroFieldConfig {

  /** Create a new one based on a json JValue.
    */
  def apply(config: JValue): AvroFieldConfig = {
    implicit val jsonFormats = org.json4s.DefaultFormats
    val config = AvroFieldConfig(
      getStructFieldFromAvroElement(config),
      (config \ "default")
    )

    config.checkDefaultValue
    config
  }
}

