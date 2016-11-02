package com.cars.bigdata.turbocow

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.json4s.JsonAST.JNull

import scala.annotation.tailrec
import SQLContextUtil._

/** DataFrame helper functions.
  */
object DataFrameUtil
{

  val changeSchemaErrorField = "__TURBOCOW_DATAFRAMEUTIL_CHANGESCHEMA_PROCESSFIELDS_ERRORS__"

  case class DataFrameOpResult(
    goodDF: DataFrame, 
    errorDF: DataFrame)

  // New methods for DataFrame you get when importing DataFrameUtil._ :
  implicit class DataFrameAdditions(val df: DataFrame) {

    lazy val sqlCtx: SQLContext = df.sqlContext

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

    /** Convert a dataframe so its schema is all strings.
      * All fields should be convertible to a string or this will error.
      */
    def convertToAllStrings(): DataFrame = {
    
      val fields = df.schema.fields

      @tailrec
      def process(dfIn: DataFrame, schemaFields: Seq[StructField]): DataFrame = {
        if (schemaFields.isEmpty) dfIn
        else {
          val sf = schemaFields.head
          val modDF = dfIn.withColumn(sf.name, dfIn(sf.name).cast(StringType))
          process(modDF, schemaFields.tail)
        }
      }
      process(df, fields.toSeq)
    }

    /** Add a column giving a default value
      */
    def addColumnWithDefaultValue(fieldConfig: AvroFieldConfig): 
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
      schema: List[AvroFieldConfig]): 
      DataFrame = { 

      val inputFieldNames: List[String] = df.schema.fields.map{ _.name }.toList

      // Default the fields that don't yet exist in the dataframe:
      val fieldsNotInInputDF = schema.filter{ fieldConfig => !inputFieldNames.contains(fieldConfig.structField.name) }
      @tailrec
      def recursiveAddField(df: DataFrame, schema: List[AvroFieldConfig]): DataFrame = {
        
        if (schema.isEmpty) df
        else {
          val newDF = df.addColumnWithDefaultValue(schema.head)
          recursiveAddField(newDF, schema.tail)
        }
      }
      val enrichedPlusDF = recursiveAddField(df, fieldsNotInInputDF)

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

    /** Change the schema of a dataframe to conform to a new schema.  The dataframe
      * will be modified as follows:
      * 
      *   * Missing columns will be set to null
      *   * Additional columns not in the specified schema will be filtered out
      *   * Columns that exist in both schemas but with different types will 
      *     be converted, and if not possible to convert, will be filtered out
      *     and returned as a separate dataframe.
      *
      * @return DataFrameOpResult where goodDF contains all rows where 
      *         every field converted successfully; and errorDF contains all rows
      *         where at least 1 field did not convert successfully.
      */
    def changeSchema(
      newSchema: List[AvroFieldConfig]): 
      DataFrameOpResult = {

      val stNewSchema = StructType( newSchema.map{ _.structField }.toArray )
      val oldSchema = df.schema

      case class OldNewFields(
        oldField: Option[StructField], 
        newAFC: Option[AvroFieldConfig])

      // TODO If order matters, then this may not be a good struct to create:
      val listOldNew: Seq[OldNewFields] = {
        val allOld = oldSchema.fields.map{ oldSF => 
          val newOpt = newSchema.find( _.structField.name == oldSF.name )
          OldNewFields( Option(oldSF), newOpt )
        }.toSeq
        val allNew = newSchema.flatMap{ newAFC => 
          // only process this field if not already in allOld:
          if (allOld.find{ e => (e.oldField.nonEmpty && (e.oldField.get.name == newAFC.structField.name)) }.nonEmpty ) {
            None
          }
          else {
            val oldOpt = oldSchema.fields.find( _.name == newAFC.structField.name)
            Option(OldNewFields( oldOpt, Option(newAFC) ))
          }
        }.toSeq
        (allOld ++ allNew)
      }

      val tempField = "__TURBOCOW_DATAFRAMEUTIL_CHANGESCHEMA_PROCESSFIELDS_TEMPFIELD__"
      val errorField = changeSchemaErrorField

      // udf function to add an error message to the error field
      val addToErrorFieldUdf = udf { (existingErrorField: String, addition: String) =>
        if (existingErrorField == null || existingErrorField.isEmpty) addition
        else existingErrorField + "; " + addition
      }

      @tailrec
      def processFields( 
        fields: Seq[OldNewFields], 
        dfr: DataFrameOpResult ): 
        DataFrameOpResult = {

        if (fields.isEmpty) dfr
        else {
          val oldNew = fields.head

          val newDFR = {
            if (oldNew.oldField.nonEmpty && oldNew.newAFC.nonEmpty) {
              // old & new schema contains the same field name
              val old = oldNew.oldField.get
              val nu = oldNew.newAFC.get
              val name = old.name
              if ( old.dataType != nu.structField.dataType ) {
                val dfTemp = {
                  if (nu.structField.dataType == NullType) throw new Exception("can't cast anything to a NullType (schemas with NullType types not supported in this Spark version).")
                  else {
                    dfr.goodDF.withColumn(
                      tempField, 
                      col(name).cast(nu.structField.dataType))
                  }
                }
                val split = dfTemp.split( col(tempField).isNotNull )
                val errString = s"could not convert field '${name}' to '${nu.structField.dataType}'"

                val posMod = split.positive
                  .drop(name)
                  .withColumnRenamed(tempField, name)

                println("RRRRRRRRRRRRRRRR posMod schema = ")
                posMod.schema.fields.foreach{println}

                // convert errors (negative) to all-strings
                val negAllStrings = split.negative.drop(tempField).convertToAllStrings()
                val negMod = negAllStrings
                  .withColumn(
                    errorField,
                    addToErrorFieldUdf(col(errorField), lit(errString)))

                println("RRRRRRRRRRRRRRR negMod schema = ")
                negMod.schema.fields.foreach{println}

                // now process the error side of the input.
                // (needs slightly different processing, don't combine with above code)
                val errorDFProcessed = {
                  val dfErrorTemp = dfr.errorDF.withColumn(
                    tempField,
                    col(name).cast(nu.structField.dataType))
                  val split = dfErrorTemp.split( col(tempField).isNotNull )

                  // drop the tempfield
                  val posMod = split.positive.drop(tempField)

                  println("SSSSSSSSSSSSS posMod schema = ")
                  posMod.schema.fields.foreach{println}

                  val negMod = {
                    val dropped = split.negative.drop(tempField)
                    val errorUpdated = dropped.withColumn(
                      errorField,
                      addToErrorFieldUdf(col(errorField), lit(errString)))
                    errorUpdated
                  }

                  println("SSSSSSSSSSSSS negMod schema = ")
                  negMod.schema.fields.foreach{println}

                  // now merge back together
                  posMod.unionAll(negMod)
                }

                DataFrameOpResult(posMod, errorDFProcessed.unionAll(negMod))
              }
              else dfr
            }
            else if (oldNew.oldField.isEmpty && oldNew.newAFC.nonEmpty) {
              // add the new field as null
              DataFrameOpResult(
                df.withColumn(oldNew.newAFC.get.structField.name, lit(null)),
                sqlCtx.createEmptyDataFrame(stNewSchema))
            }
            else {
              throw new Exception("TODO")
            }
          }

          processFields(fields.tail, newDFR)
        }
      }

      val nullString: String = null
      val dfWithErrorField = df.withColumn(errorField, lit(nullString))
      val allStringSchema = StructType( dfWithErrorField.schema.fields.map{ _.copy(dataType=StringType) }.toArray )
      processFields(
        listOldNew, 
        DataFrameOpResult(
          dfWithErrorField, 
          df.sqlContext.createEmptyDataFrame(allStringSchema) ) )
    }
  }
}
