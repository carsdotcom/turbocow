package com.cars.bigdata.turbocow

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.util.Try;

object AvroOutputWriter {

  /** Output data to avro using a specific Avro schema file.
    *
    * @param rdd        RDD to write out
    * @param outputDir  the dir to write to (hdfs:// typically)
    * @param schemaPath path to schema file
    * @param sc         spark context
    */
  def write(
    rdd: RDD[Map[String, String]],
    schemaPath: String,
    outputDir: String,
    sc: SparkContext): 
    Unit = {

    // get the list of field names from avro schema
    val schema: List[AvroFieldConfig] = getAvroSchemaFromHdfs(schemaPath, sc)

    write(rdd, schema, outputDir, sc)
  }

  /** Output data to avro - with list of fields for the schema (useful for testing)
    *
    * @param rdd       RDD to write out
    * @param outputDir the dir to write to (hdfs:// typically)
    * @param schema    list of fields to write
    * @param sc        spark context
    */
  def write(
    rdd: RDD[Map[String, String]],
    schema: List[AvroFieldConfig],
    outputDir: String,
    sc: SparkContext):
    Unit = {

    // Loop through enriched record fields, and extract the value of each field 
    // in the order of schema list (so the order matches the Avro schema). 
    // Convert to the correct type as well.
    val rowRDD: RDD[Row] = rdd.map { record =>
      val vals: List[Any] = schema.map{ fieldConfig =>
        val v = record.get(fieldConfig.structField.name)
        if (v.isDefined) {
          convertToType(v.get, fieldConfig.structField).get  // TODOTODO this may throw!  Shouldn't this be rejected if can't convert the value to the proper type?  How to do that here?
        }
        else {
          // add the default value
          fieldConfig.getDefaultValue
        }
      }
      Row.fromSeq(vals)
    }

    // create a dataframe of RDD[row] and Avro schema
    val structTypeSchema = StructType(schema.map{ _.structField }.toArray)
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.createDataFrame(rowRDD, structTypeSchema).repartition(10)

    //println("================================= dataFrame = ")
    //dataFrame.printSchema
    //dataFrame.show

    dataFrame.write.format("com.databricks.spark.avro").save(outputDir)
  }

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
    getAvroSchema(jsonSchema, sc)
  }

  /** Process AvroSchema (schema as string)
    *
    * @param jsonSchema the schema (from a .avsc file which is just JSON)
    * @param sc SparkContext
    */
  def getAvroSchema(
    jsonSchema: String,
    sc: SparkContext):
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
      case s: String => throw new Exception("unrecognized avro type in avro schema: "+s)
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
    val typeList = fields.children.map{ typeJval =>
      getDataTypeFromString(typeJval.extract[String])
    }

    val nullable = typeList.contains(NullType)

    val filtered = typeList.filter( _ != NullType )
    val dataType = filtered.size match {
      case 0 => if (nullable) NullType; else throw new Exception("couldn't determine type for avro field name="+name)
      case 1 => filtered.head
      case i: Int => throw new Exception("not able to parse type list for avro field: "+name)
    }

    println(s"========== name = $name, dataType=$dataType, nullable=$nullable")
    StructField(name, dataType, nullable)
  }

  /** Convert a string to a datatype as specified.  
    * 
    * Most types are supported.
    * 
    * @todo pull out core type conversion logic into another function and wrap with this
    *
    * @param  structField
    * @return Try[ Any ] (String, Int, etc..).  Will be an Failure if an exception 
    *         is thrown, either through our checks or checks in the .toX conversion.
    */
  def convertToType(string: String, structField: StructField): Try[Any] = Try {

    // Check for null value and throw if not allowed.
    if (string == null && !(structField.dataType==NullType || structField.nullable) ) {
      throw new Exception("attempt to store null value into a field that is non-nullable")
    }

    // If value is actually null, just return null
    if (string == null) null
    else {
      // value is not null

      // For numeric types, trim the input and check for these slip through the
      // [type].toString conversion because they are valid numeric postfixes in
      // scala.
      val trimmedStr = structField.dataType match {
        case IntegerType | LongType | FloatType | DoubleType => {
          val t = string.trim()
          if (List( 'd', 'D', 'f', 'F', 'l', 'L').contains(t.last)) {
            throw new NumberFormatException(s"can't convert '$string' to ${structField.dataType.toString} type. (No alphanumeric characters are allowed in numeric fields.)")
          }
          else t
        }
        case _ => string;
      }

      // Do the core conversion.
      structField.dataType match {
        case StringType => string
        case IntegerType => trimmedStr.toInt
        case LongType => trimmedStr.toLong
        case FloatType => trimmedStr.toFloat
        case DoubleType => trimmedStr.toDouble
        case BooleanType => string.trim.toBoolean // boolean can be safely trimmed (it is not above)
        case NullType => trimmedStr match {
          case null => null
          case _ => throw new Exception("attempt to convert non-null value into 'NullType'.")
        }
        case _ => throw new Exception("unsupported type: "+structField.toString)
      }
    }
  }
}

