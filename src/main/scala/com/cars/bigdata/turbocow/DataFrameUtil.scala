package com.cars.bigdata.turbocow

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.annotation.tailrec

/** DataFrame helper functions.
  */
object DataFrameUtil
{

  /** Set default values for null fields in a dataframe.  
    * Returns a new dataframe with null fields replaced according to the schema's
    * defaults.
    */
  def setDefaultValues(
    enrichedDF: DataFrame, 
    schema: List[AvroFieldConfig]): 
    DataFrame = { 

    val boolSchema = schema.filter{ fc => fc.structField.dataType == BooleanType }
    val safeSchema = schema.filter{ fc => fc.structField.dataType != BooleanType }

    val defaultsMap = safeSchema.map{ fc => 
      ( fc.structField.name, fc.getDefaultValue )
    }.toMap

    println("defaultsMap = "+defaultsMap)

    // First default the non-bools.  That's easy:
    val defaultDF = enrichedDF.na.fill(defaultsMap)

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
        val newDF = df.withColumn(tempField, convertToInt(col(name)))
          .drop(name)
          .withColumnRenamed(tempField, name)

        recursiveDefault(newDF, schema.tail)
      }
    }
    recursiveDefault(defaultDF, boolSchema)
  }
  
}
