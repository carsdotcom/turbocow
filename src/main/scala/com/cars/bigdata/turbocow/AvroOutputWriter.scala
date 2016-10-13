package com.cars.bigdata.turbocow

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.util.Try
import utils._
import org.apache.hadoop.fs.{FileSystem, Path}
import AvroOutputWriter._
import org.apache.spark.storage.StorageLevel

class AvroOutputWriter(
  sc: SparkContext,
  val avroWriterConfig: AvroOutputWriterConfig = AvroOutputWriterConfig()
) 
{

  /** Output data to avro using a specific Avro schema file.
    *
    * @param rdd              RDD to write out
    * @param outputDir        the dir to write to (hdfs:// typically)
    * @param schemaPath       path to schema file
    * @return RDD of rejected records due to data types.  The RDD will
    *         be empty if no records are rejected.
    */
  def write(
    rdd: RDD[Map[String, String]],
    schemaPath: String,
    outputDir: String):
    RDD[Map[String, String]] = {

    // get the list of field names from avro schema
    val schema: List[AvroFieldConfig] = getAvroSchemaFromHdfs(schemaPath, sc)

    write(rdd, schema, outputDir)
  }

  /** Output data to avro - with list of fields for the schema (useful for testing)
    * 
    * @param rdd       RDD to write out
    * @param outputDir the dir to write to (hdfs:// typically)
    * @param schema    list of fields to write
    * @return RDD of rejected records due to data types.  The RDD will
    *         be empty if all records write successfully.  Each record will have an 
    *         additional field named AvroOutputWriter.avroOutputWriterTypeErrorMarker,
    *         with all of the type errors separated by semicolons.
    */
  def write(
    rdd: RDD[Map[String, String]],
    schema: List[AvroFieldConfig],
    outputDir: String): 
    RDD[Map[String, String]] = {

    val writerConfig = avroWriterConfig // make local ref so whole obj doesn't get serialized (which includes the sc and therefore can't be serialized)
    val errorMarker = avroTypeErrorMarker

    // Loop through enriched record fields, and extract the value of each field 
    // in the order of schema list (so the order matches the Avro schema). 
    // Convert to the correct type as well.
    val anyRDD: RDD[Map[String, Any]] = rdd.map{ record => 

      var errors = List.empty[String]

      val newRecord: Map[String, Any] = schema.map{ fieldConfig =>

        val key = fieldConfig.structField.name
        val v = record.get(key)
        val value = {
          if (v.isDefined) {
            try{
              convertToType(v.get, fieldConfig.structField, writerConfig).get
            }
            catch {
              case e: EmptyStringConversionException => {
                //println(s"Detected empty string in '${fieldConfig.structField.name}' when trying to convert; using default value.")
                fieldConfig.getDefaultValue
              }
              case e: Throwable => {
                errors = errors :+ e.getMessage
                v.get.toString
              }
            }
          }
          else {
            // add the default value
            fieldConfig.getDefaultValue
          }
        }

        (key, value)
      }.toMap

      if (errors.nonEmpty) newRecord + (errorMarker-> errors.mkString("; "))
      else newRecord
    }

    // Now filter out the error records for later returning.
    val errorRDD: RDD[Map[String, String]] = anyRDD.filter{ 
      _.get(errorMarker).nonEmpty
    }.map{ record =>  
      // convert to strings - can't write out if the type is incorrect
      record.map{ case(k,v) => v match {
        case null => (k, null)
        case _ => (k, v.toString) 
      }}
    }

    // Filter them out in the main rdd.
    val rowRDD: RDD[Row] = anyRDD.filter {
      _.get(errorMarker).isEmpty
    }.map{ record =>
      val vals: List[Any] = schema.map{ fieldConfig =>
        val field = fieldConfig.structField.name
        record.getOrElse(field, throw new Exception(s"couldn't find field name $field in anyRDD"))
      }
      Row.fromSeq(vals)
    }

    // create a dataframe of RDD[row] and Avro schema
    val structTypeSchema = StructType(schema.map{ _.structField }.toArray)
    val sqlContext = new SQLContext(sc)

    // this gives 18,000 or something like it
    //val numPartitions = rowRDD.partitions.size
    //println("Avro writer: rowRDD.partitions = "+numPartitions)
    val dataFrame = sqlContext.createDataFrame(rowRDD, structTypeSchema).repartition(30)
    dataFrame.persist(StorageLevel.MEMORY_AND_DISK_SER)

    //println("================================= dataFrame = ")
    //dataFrame.printSchema
    //dataFrame.show

    // Ensure the dir can be written to by deleting it.
    // It is up to the operator to ensure it is safe to overwrite this dir.
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val outputDirPath = new Path(outputDir)
    if (fs.exists(outputDirPath)) {
      fs.delete(outputDirPath, true)
    }

    // lastly, write it out
    dataFrame.write.format("com.databricks.spark.avro").save(outputDir)

    // Unpersist anything we are done with.
    dataFrame.unpersist(blocking=true)
    rowRDD.unpersist(blocking=true)
    anyRDD.unpersist(blocking=true)

    // return the errors
    errorRDD
  }
}

object AvroOutputWriter {

  val avroTypeErrorMarker = "____TURBOCOW_AVROOUTPUTWRITER_TYPE_ERROR____"

  /** Process AvroSchema from HDFS
    *
    * @param hdfsPath
    * @param sc SparkContext
    */
  def getAvroSchemaFromHdfs(
    hdfsPath: String,
    sc: SparkContext):
    List[AvroFieldConfig] = {

    val jsonSchema = sc.textFile(hdfsPath).collect().mkString("")
    getAvroSchema(jsonSchema)
  }

  /** Process AvroSchema (schema as string)
    *
    * @param jsonSchema the schema (from a .avsc file which is just JSON)
    */
  def getAvroSchema(
    jsonSchema: String):
    List[AvroFieldConfig] = {

    println("=================== jsonSchema = "+jsonSchema)
    val parsedSchema = parse(jsonSchema)

    implicit val formats = org.json4s.DefaultFormats

    // collect fields list from avro schema
    val fields = (parsedSchema \ "fields").children

    fields.map { eachChild => AvroFieldConfig(eachChild) }
  }

  /** get the DataType type from a string description
    * 
    */
  def getDataTypeFromString(typeStr: String): DataType = {
    typeStr match {
      case "string" => StringType
      case "int" => IntegerType
      case "long" => LongType
      case "float" => FloatType
      case "double" => DoubleType
      case "boolean" => BooleanType
      case "null" => NullType
      case s: String => throw new Exception("unrecognized avro type in avro schema: \""+s+"\"")
    }
  }

  /** get a structfield from parsed avro element from the config, ie. a parsed:
    * 
    *      {
    *        "name": "IntField",
    *        "type": [ "null", "int" ],
    *        "default": 0
    *      }
    * 
    * @return StructField
    */
  def getStructFieldFromAvroElement(fieldConfig: JValue): StructField = {
    
    val name = JsonUtil.extractValidString(fieldConfig\"name").getOrElse(throw new Exception("could not find valid 'name' element in avro field config"))
    val fields = (fieldConfig \ "type").toOption.getOrElse(throw new Exception(s"avro field configuration for '$name' is missing the 'type' array"))
    implicit val jsonFormats = org.json4s.DefaultFormats
    val typeList = fields match {
      case j: JString => List(getDataTypeFromString(j.extract[String]))
      case j: JArray => {
        j.children.map{ typeJval =>
          getDataTypeFromString(typeJval.extract[String])
        }
      }
      case _ => throw new RuntimeException(s"invalid 'type' field for field named '$name'")
    }

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

    //println(s"========== name = $name, dataType=$dataType, nullable=$nullable")
    StructField(name, dataType, nullable)
  }

  /** Convert a string to a datatype as specified.  
    * 
    * Most types are supported.
    * 
    * @throws NumberFormatException if string data cannot be converted to numeric value
    * @throws Exception if the data cannot be converted, for another reason.
    * @throws Exception if attempting to store null value into non-nullable field.
    *
    * @todo pull out core type conversion logic into another function and wrap with this?
    *
    * @param  structField
    * @return Try[ Any ] (String, Int, etc..).  Will be an Failure if an exception 
    *         is thrown, either through our checks or checks in the .toX conversion.
    */
  def convertToType(
    string: String, 
    structField: StructField, 
    conf: AvroOutputWriterConfig = AvroOutputWriterConfig()): 
    Try[Any] = Try {

    // Check for null value and throw if not allowed.
    if (string == null && !(structField.dataType==NullType || structField.nullable) ) {
      throw new Exception("attempt to store null value into a field that is non-nullable")
    }

    // If value is actually null, just return null
    if (string == null) null
    else {
      // value is not null

      // For numeric types, trim the input and check for these things that slip 
      // through the [type].toString conversion because they are valid numeric 
      // postfixes in scala.
      val trimmedStr = structField.dataType match {
        case IntegerType | LongType | FloatType | DoubleType => {
          val t = string.optionalTrim(conf.alwaysTrimNumerics)
          if (t.nonEmpty && List( 'd', 'D', 'f', 'F', 'l', 'L').contains(t.last)) {
            throw new NumberFormatException(s"can't convert '$string' to ${structField.dataType.toString} type. (No alphanumeric characters are allowed in numeric fields.)")
          }
          else t
        }
        // Optionally trim bools and strings too, per the config.
        case BooleanType => string.optionalTrim(conf.alwaysTrimBooleans)
        case StringType => string.optionalTrim(conf.alwaysTrimStrings)
        case _ => string;
      }

      // For numeric and boolean types, if the config specifies, then throw a 
      // EmptyStringConversionException (a custom exception) so it can be potentially 
      // handled differently by the caller.
      structField.dataType match {
        case IntegerType | LongType | FloatType | DoubleType | BooleanType => {
          if (trimmedStr.trim == "") { // (may not have trimmed it above)
            throw new EmptyStringConversionException("cannot convert empty string in field '${structField.name}' to numeric or boolean value")
          }
        }
        case _ => ;
      }

      // Do the core conversion.
      try {
        structField.dataType match {
          case StringType => string
          case IntegerType => trimmedStr.toInt
          case LongType => trimmedStr.toLong
          case FloatType => trimmedStr.toFloat
          case DoubleType => trimmedStr.toDouble
          case BooleanType => trimmedStr.toBoolean
          case NullType => trimmedStr match {
            case null => null
            case _ => throw new Exception(s"attempt to convert non-null value in '${structField.name}' into 'NullType'.")
          }
          case _ => throw new Exception(s"unsupported type: '${structField.dataType.toString}'")
        }
      }
      catch {
        // for numeric and boolean conversions, add more info to say why
        case e: java.lang.IllegalArgumentException => {
          val message = s"could not convert value in '${structField.name}' to a '${structField.dataType.toString}'."
          throw new java.lang.IllegalArgumentException(message, e)
        }
        case e: java.lang.Throwable => throw e
      }
    }
  }


  /** Helper to convert an existing schema to all-strings, suitable for writing
    * a record that has data values incompatible with the avro types.
    *
    * @param  inputSchema
    * @param  forceNullable (default true) if true, will set all nullable fields to true.
    *                       It is safest to allow nullable when writing a rejected record.
    * @return a modified schema with only strings
    */
  def convertToAllStringSchema(
    inputSchema: List[AvroFieldConfig],
    forceNullable: Boolean = true ):
    List[AvroFieldConfig] = {

    inputSchema.map{ fc => 
      val newNullable = { if (forceNullable) true; else fc.structField.nullable }
      val newStructField = fc.structField.copy(
        dataType = StringType, 
        nullable = newNullable)

      val newDefaultValue: JValue = fc.defaultValue match {
        case v: JString => JString(v.values.toString)
        case v: JInt => JString(v.values.toString)
        case v: JDouble => JString(v.values.toString)
        case v: JBool => JString(v.values.toString)
        case JNull => JNull
        // This should never happen because a valid default value is enforced elsewhere:
        case v: JValue => throw new Exception("unexpected jvalue: "+v.toString)
      }

      AvroFieldConfig(newStructField, newDefaultValue)
    }
    
  }

}

class EmptyStringConversionException(message: String) extends RuntimeException(message)

