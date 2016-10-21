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
import DataFrameUtil._

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
  def writeEnrichedRDD(
    rdd: RDD[Map[String, String]],
    schemaPath: String,
    sqlContext: SQLContext,
    outputDir: String):
    RDD[Map[String, String]] = {

    // get the list of field names from avro schema
    val schema: List[AvroFieldConfig] = getAvroSchemaFromHdfs(schemaPath, sc)

    writeEnrichedRDD(rdd, schema, sqlContext, outputDir)
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
  def writeEnrichedRDD(
    rdd: RDD[Map[String, String]],
    schema: List[AvroFieldConfig],
    sqlContext: SQLContext,
    outputDir: String): 
    RDD[Map[String, String]] = {

    val (goodDataFrame: DataFrame, errorRDD: RDD[Map[String, String]]) = 
      convertEnrichedRDDToDataFrame(
        rdd, 
        StructType( schema.map{ _.structField }.toArray ), 
        sqlContext, 
        avroWriterConfig)

    val dataFrame = goodDataFrame.setDefaultValues(schema)
    dataFrame.persist(StorageLevel.MEMORY_AND_DISK_SER)

    //println("================================= dataFrame = ")
    //dataFrame.printSchema
    //dataFrame.show

    write(dataFrame, new Path(outputDir))

    // Unpersist anything we are done with.
    dataFrame.unpersist(blocking=true)

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

    fields.map { eachChild => 
      val afc = AvroFieldConfig(eachChild) 

      // Float fields are disallowed due to https://issues.apache.org/jira/browse/SPARK-14081
      if (afc.structField.dataType == FloatType) throw new RuntimeException(""""float" types are not allowed in the avro schema due to a Spark bug.  Please change all "float" types to "double".""")

      afc
    }
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
    
    val asf = AvroSchemaField(fieldConfig)
    asf.toStructField
  }

  /** Transform an RDD of string data into correctly typed data according to the
    * schema.
    * Missing values are NOT defaulted but instead NULL is written.
    */
  def createTypedEnrichedRDD(
    rdd: RDD[Map[String, String]], 
    schema: StructType,
    errorMarker: String,
    writerConfig: AvroOutputWriterConfig = AvroOutputWriterConfig()):
    RDD[Map[String, Any]] = {

    rdd.map{ record => 

      var errors = List.empty[String]

      val newRecord: Map[String, Any] = schema.fields.flatMap{ structField =>

        val key = structField.name
        val v = record.get(key)
        val value: Option[Any] = {
          if (v.isDefined) {
            try{
              Option(convertToType(v.get, structField, writerConfig).get)
            }
            catch {
              case e: EmptyStringConversionException => {
                //println(s"Detected empty string in '${structField.name}' when trying to convert; using default value.")
                None
              }
              case e: Throwable => {
                errors = errors :+ e.getMessage
                Option(v.get.toString)
              }
            }
          }
          else { 
            None
          }
        }

        // Only add new keys if the value is non-null.  Null values are added
        // later when converted into the dataframe.
        if (value.nonEmpty) {
          Option((key, value.get))
        }
        else {
          None
        }
      }.toMap

      if (errors.nonEmpty) newRecord + (errorMarker-> errors.mkString("; "))
      else newRecord
    }
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
    if (string == null) { 
      null
    }
    else {
      // value is not null

      // For numeric types, trim the input and check for these things that slip 
      // through the [type].toString conversion because they are valid numeric 
      // postfixes in scala.
      val trimmedStr = structField.dataType match {
        case IntegerType | LongType | FloatType | DoubleType => {
          val t = string.optionalTrim(conf.alwaysTrimNumerics)
          if (t.nonEmpty && List( 'd', 'D', 'f', 'F', 'l', 'L').contains(t.last)) {
            throw new NumberFormatException(s"can't convert '$string' in field '${structField.name}' to ${structField.dataType.toString} type. (No alphanumeric characters are allowed in numeric fields.)")
          }
          else t
        }
        // Optionally trim bools and strings too, per the config.
        case BooleanType => string.optionalTrim(conf.alwaysTrimBooleans)
        case StringType => string.optionalTrim(conf.alwaysTrimStrings)
        case _ => string
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
      val result = try {
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
          val message = s"""could not convert value '$string' in field '${structField.name}' to a '${structField.dataType.toString}'."""
          throw new java.lang.IllegalArgumentException(message, e)
        }
        case e: java.lang.Throwable => throw e
      }

      result
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

  /** Write out a dataframe to Avro at the specified path.
    * 
    */
  def write(df: DataFrame, outputDir: Path) = {

    val sc = df.sqlContext.sparkContext

    // Ensure the dir can be written to by deleting it.
    // It is up to the client to ensure it is safe to overwrite this dir.
    val fs = FileSystem.get(sc.hadoopConfiguration)
    if (fs.exists(outputDir)) {
      fs.delete(outputDir, true)
    }

    // lastly, write it out
    df.write.format("com.databricks.spark.avro").save(outputDir.toString)
  }

  /** Convert the enriched RDD map to a dataframe.  
    * Records that couldn't be converted due to type conversion issues are returned
    * as an RDD of the same type.
    *
    * @return (DataFrame, RDD) where the dataframe is the good records, and 
    *         the RDD is the bad records that need to be handled separately due
    *         to type conversion issues.
    */
  def convertEnrichedRDDToDataFrame(
    enrichedRDD: RDD[Map[String, String]],
    schema: StructType,
    sqlContext: SQLContext,
    writerConfig: AvroOutputWriterConfig = AvroOutputWriterConfig()): 
    (DataFrame, RDD[Map[String, String]]) = {

    val errorMarker = avroTypeErrorMarker

    // create schema that ensures every field is nullable.
    if (schema == null || schema.fields == null) throw new RuntimeException("schema must not be null in convertEnrichedRDDToDataFrame()")
    if (schema.fields.isEmpty) throw new RuntimeException("schema must not be empty in convertEnrichedRDDToDataFrame()")
    val safeSchema = StructType( schema.fields.map{ _.copy(nullable=true) } )

    // Loop through enriched record fields, and extract the value of each field 
    // in the order of schema list (so the order matches the Avro schema). 
    // Convert to the correct type as well.
    val anyRDD: RDD[Map[String, Any]] = createTypedEnrichedRDD(enrichedRDD, safeSchema, errorMarker, writerConfig)

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
      val vals: Seq[Any] = safeSchema.fields.map{ structField =>
        val field = structField.name
        record.getOrElse(field, null)
      }
      Row.fromSeq(vals)
    }

    // this gives 18,000 or something like it
    //val numPartitions = rowRDD.partitions.size
    //println("Avro writer: rowRDD.partitions = "+numPartitions)
    val dataFrame = sqlContext.createDataFrame(rowRDD, safeSchema).repartition(30)

    // unpersist our temp RDDs
    rowRDD.unpersist(blocking=true)
    anyRDD.unpersist(blocking=true)
   
    // return the tuple
    (dataFrame, errorRDD)
  }

}

class EmptyStringConversionException(message: String) extends RuntimeException(message)

