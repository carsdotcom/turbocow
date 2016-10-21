package com.cars.bigdata.turbocow

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.json4s.JsonAST.JNull

import scala.annotation.tailrec

/** DataFrame helper functions.
  */
object DataFrameUtil
{
  implicit class DataFrameAdditions(val df: DataFrame) {

    /** Split a dataframe.  Does 2 filters, one with the condition and
      * one with the condition negated with not().
      *
      * @return (filtered DataFrame, negatively-filtered DataFrame)
      */
    case class SplitDataFrame(positive: DataFrame, negative: DataFrame)
    def split(condition: Column): SplitDataFrame = {
      SplitDataFrame( df.filter( condition ), df.filter( not(condition) ) )
    }

    /** Get the data type for a field name in a schema.
      */
    def getDataTypeForField(field: String): Option[DataType] = {
      val schema = df.schema
      val structFieldOpt = schema.fields.find{ _.name == field }
      if (structFieldOpt.nonEmpty) 
        Option(structFieldOpt.get.dataType)
      else
        None
    }

    // NOTE THIS DOES NOT WORK AND CANNOT BE MADE TO WORK.
    // DELETE THIS WARNING AFTER SPARK 2.0 COMES OUT AND YOU CAN USE DF.NA.fill()
    // SAFELY...
    ///** Change double type to float.  Workaround for problem with df.na.fill()
    //  * that changes float types to doubles, inexplicably.
    //  * see https://issues.apache.org/jira/browse/SPARK-14081
    //  */
    //def retypeDoubleToFloat(floatField: String): DataFrame = {
    //  val copyAsFloat = udf { (v: Float) => v }
    //  val tempCol = "____TURBOCOW_DATAFRAMEUTIL_DATAFRAMEADDITIONS_RETYPEDOUBLETOFLOAT_TEMP___"
    //
    //  //df.withColumn(tempCol, copyAsFloat(col(floatField).cast(FloatType)))
    //  df.withColumn(tempCol, df(floatField).cast(FloatType))
    //    .drop(floatField)
    //    .withColumnRenamed(tempCol, floatField)
    //}
  }

  /** Add a column giving a default value
    */
  def addColumnWithDefaultValue(df: DataFrame, fieldConfig: AvroFieldConfig): 
    DataFrame = {
  
    val name = fieldConfig.structField.name
  
    if (fieldConfig.defaultValue == JNull) {
      fieldConfig.structField.dataType match {
        case StringType => df.withColumn(name, lit(null).cast(StringType))
        case IntegerType => df.withColumn(name, lit(null).cast(IntegerType))
        case LongType => df.withColumn(name, lit(null).cast(LongType))
        case FloatType => df.withColumn(name, lit(null).cast(FloatType))
        case DoubleType => df.withColumn(name, lit(null).cast(DoubleType))
        case BooleanType => df.withColumn(name, lit(null).cast(BooleanType))
        case NullType => df.withColumn(name, lit(null))
      }
    }
    else {
      fieldConfig.structField.dataType match {
        case StringType => df.withColumn(name, lit(fieldConfig.getDefaultValueAs[String].get))
        case IntegerType => df.withColumn(name, lit(fieldConfig.getDefaultValueAs[Int].get))
        case LongType => df.withColumn(name, lit(fieldConfig.getDefaultValueAs[Long].get))
        case FloatType => df.withColumn(name, lit(fieldConfig.getDefaultValueAs[Float].get))
        case DoubleType => df.withColumn(name, lit(fieldConfig.getDefaultValueAs[Double].get))
        case BooleanType => df.withColumn(name, lit(fieldConfig.getDefaultValueAs[Boolean].get))
        case NullType => df.withColumn(name, lit(fieldConfig.getDefaultValueAs[Null].get))
      }
    }
  }

  /** Set default values for null or missing fields in a dataframe.  
    * Returns a new dataframe with null fields replaced according to the schema's
    * defaults.
    */
  def setDefaultValues(
    enrichedDF: DataFrame, 
    schema: List[AvroFieldConfig]): 
    DataFrame = { 

    val inputFieldNames: List[String] = enrichedDF.schema.fields.map{ _.name }.toList

    // Default the fields that don't yet exist in the dataframe:
    val fieldsNotInInputDF = schema.filter{ fieldConfig => !inputFieldNames.contains(fieldConfig.structField.name) }
    @tailrec
    def recursiveAddField(df: DataFrame, schema: List[AvroFieldConfig]): DataFrame = {
      
      if (schema.isEmpty) df
      else {
        val newDF = addColumnWithDefaultValue(df, schema.head)
        recursiveAddField(newDF, schema.tail)
      }
    }
    val enrichedPlusDF = recursiveAddField(enrichedDF, fieldsNotInInputDF)

    // Filter out the boolean fields into a separate schema to handle specially.
    def filterBoolean( afc: AvroFieldConfig ): Boolean = {
      afc.structField.dataType == BooleanType
    }
    // Also filter out null default values because a) it's impossible to set
    // something null and b) there's no reason to because it already is null.
    def filterNull( afc: AvroFieldConfig ): Boolean = {
      afc.defaultValue != JNull
    }
    val boolSchema = schema.filter{ filterBoolean }.filter{ filterNull }
    val safeSchema = schema.filterNot{ filterBoolean }.filter{ filterNull }

    // Set the defaults for the non-boolean fields.  Easy:
    val defaultsMap = safeSchema.map{ fc => 
      ( fc.structField.name, fc.getDefaultValue )
    }.toMap
    val defaultDF = enrichedPlusDF.na.fill(defaultsMap)
    // na.fill changes Float data types to Double: see https://issues.apache.org/jira/browse/SPARK-14081 (fix in Spark 2.0)

    // Now default the bools.  A bit more tricky due to spark 1.5 bug that 
    // disallows use of DF.na on boolean types:  https://issues.apache.org/jira/browse/SPARK-11180
    @tailrec
    def recursiveDefault(df: DataFrame, schema: List[AvroFieldConfig]): DataFrame = {
      
      if (schema.isEmpty) df
      else {
        val fieldSchema = schema.head
        val name = fieldSchema.structField.name        
        
        val TRUE = 0
        val FALSE = 1
        val NULL = -1
        val defaultBools = udf { (boolVal: Boolean) =>
          if (boolVal == null ) fieldSchema.getDefaultValue match { case b: Boolean => b } // this should pass because we have checks on the AvroFieldConfig.defaultValue
          else if (boolVal) true
          else false
        }
        val tempField = "___TURBOCOW_TEMP_DATAFRAMEUTIL_SETDEFAULTVALUES___"
        val newDF = df.withColumn(tempField, defaultBools(col(name)))
          .drop(name)
          .withColumnRenamed(tempField, name)

        recursiveDefault(newDF, schema.tail)
      }
    }
    recursiveDefault(defaultDF, boolSchema)
  }
  
}
