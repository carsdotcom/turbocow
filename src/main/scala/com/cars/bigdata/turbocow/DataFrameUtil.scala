package com.cars.bigdata.turbocow

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.json4s.JsonAST.JNull

import scala.annotation.tailrec

/** DataFrame helper functions.
  */
object DataFrameUtil
{

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
    val boolSchema = schema.filter{ fc => fc.structField.dataType == BooleanType }
    val safeSchema = schema.filter{ fc => fc.structField.dataType != BooleanType }

    // Set the defaults for the non-boolean fields.  Easy:
    val defaultsMap = safeSchema.map{ fc => 
      ( fc.structField.name, fc.getDefaultValue )
    }.toMap
    val defaultDF = enrichedPlusDF.na.fill(defaultsMap)

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
