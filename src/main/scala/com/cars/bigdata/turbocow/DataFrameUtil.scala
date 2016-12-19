package com.cars.bigdata.turbocow

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.json4s.JsonAST.JNull

import scala.annotation.tailrec
import SQLContextUtil._
import org.apache.spark.storage.StorageLevel._

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
          val modDF = { 
            if (sf.dataType != StringType)
              dfIn.withColumn(sf.name, dfIn(sf.name).cast(StringType))
            else dfIn
          }
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
      val defaultsMap = safeSchema.map{ afc => 
        ( afc.structField.name, afc.getDefaultValue )
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
          val newDF = df.withColumn(name, defaultBools(col(name)))

          recursiveDefault(newDF, schema.tail)
        }
      }
      recursiveDefault(defaultDF, boolSchema)
    }

    /** Change the schema of a dataframe to conform to a new schema.  The dataframe
      * will be modified as follows:
      * 
      *   * Missing columns will be set to null
      *   * Additional columns not in the specified schema will be dropped.
      *   * Columns that exist in both schemas but with different types will 
      *     be converted, and if not possible to convert, will be filtered out
      *     and returned as a separate dataframe.
      *
      * @return DataFrameOpResult where goodDF contains all rows where 
      *         every field converted successfully; and errorDF contains all rows
      *         where at least 1 field did not convert successfully.
      *         ErrorDF will be an all-string schema in order to preserve the 
      *         input data that may have not converted properly.
      */
    def changeSchema(
      newSchema: Seq[AvroFieldConfig]): 
      DataFrameOpResult = {

      val stNewSchema = StructType( newSchema.map{ _.structField }.toArray )
      val oldSchema = df.schema

      case class OldNewFields(
        oldField: Option[StructField], 
        newAFC: Option[AvroFieldConfig])

      // Create a list of case classes that include each field mapping.
      // If either is None, that means it doesn't exist in the other side.
      // TODO If order matters, then this may not be a good struct to create:
      println("changeSchema: 0")
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
            Option(OldNewFields( None, Option(newAFC) ))
          }
        }.toSeq
        (allOld ++ allNew)
      }
      println("changeSchema: 1")

      val tempField = "__TURBOCOW_DATAFRAMEUTIL_CHANGESCHEMA_PROCESSFIELDS_TEMPFIELD__"
      val errorField = changeSchemaErrorField

      println("changeSchema: 2")

      var count = 0
      @tailrec
      def processField( 
        fields: Seq[OldNewFields], 
        dfIn: DataFrame ): 
        DataFrame = {

        count += 1

        if (fields.isEmpty) dfIn
        else {
          val headON = fields.head

          println("===changeSchema.processField(); field = " +headON + "; count = "+count)

          //println("on.oldField = "+on.oldField)
          //println("on.newAFC = "+on.newAFC)

          val newDF = {

            if (headON.oldField.nonEmpty && headON.newAFC.nonEmpty) {
              // old & new schema contains the same field name
              val old = headON.oldField.get
              val nu = headON.newAFC.get
              val nuType = nu.structField.dataType
              val name = old.name
              if ( old.dataType != nuType ) {
                println(">>>>>>>> datatypes are different.")

                println(">>>>>>>> splitting on cast results to "+nuType)
                val goodSplit = dfIn.split( col(name).cast(nuType).isNotNull )

                // For all the successful casts, actually write it now.
                println(s">>>>>>>> posmod - dropping $name, renaming $tempField to $name")
                val goodSplitPosMod = goodSplit.positive.withColumn(
                  name, 
                  col(name).cast(nuType))

                //println("RRRRRRRRRRRRRRRR goodSplitPosMod schema = ")
                //goodSplitPosMod.schema.fields.foreach{println}

                // Convert errors (negative) to all-strings schema and add error note
                println(s">>>>>>>> goodSplit.negative.drop($tempField) and convertToAllStrings")
                val errString = s"could not convert value in field '${name}' to '${nuType}': '"
                val endQuote = "'"
                val goodSplitNegMod = goodSplit.negative
                  .withColumn(
                    errorField,
                    concat_ws( "; ", col(errorField), concat(lit(errString), col(name), lit(endQuote))))
                  .withColumn(name, lit(null).cast(nuType))

                // Now merge back together
                println(s">>>>>>>> goodSplitPosMod.safeUnionAll(goodSplitNegMod)")
                val goodMerged = goodSplitPosMod.safeUnionAll(goodSplitNegMod)

                println(">>>>>>>> >>>>>>>> returning DF...")
                goodMerged
              }
              else dfIn
            }
            else if (headON.oldField.isEmpty && headON.newAFC.nonEmpty) {
              // Add the new field as null.
              val sf = headON.newAFC.get.structField
              println(s">>>>>>>> ELSE adding new null column: ${sf.name}")
              dfIn.withColumn(sf.name, lit(null).cast(sf.dataType))
            }
            else if (headON.oldField.nonEmpty && headON.newAFC.isEmpty) {
              // Just drop the field
              val sf = headON.oldField.get
              println(s">>>>>>>> ELSE dropping ${sf.name}")
              dfIn.drop(sf.name)
            }
            else {
              throw new Exception("It should be impossible for on.oldField AND on.newAFC to both be empty.")
            }
          }

          // Persist only if the count is at 70 or above
          if (count >= 70) {
            val saveGood = (newDF ne dfIn) 
            if (saveGood) {
              println("Persisting new newDF...")
              newDF.persist(MEMORY_ONLY)
              println("size of newDF.take(1).size = "+newDF.take(1).size)
            }
            if (saveGood) {
              println("Unpersisting old dfIn...")
              dfIn.unpersist(blocking=true)
            }
          }

          processField(fields.tail, newDF)
        }
      }
      println("changeSchema: 3")

      val dfWithErrorField = df.withColumn(errorField, lit(null).cast(StringType))
      val allStringSchema = StructType( dfWithErrorField.schema.fields.map{ _.copy(dataType=StringType, nullable=true) }.toArray )
      println("changeSchema: 4")
      val result = { 

        var res = dfWithErrorField
        //res.goodDF.persist(MEMORY_ONLY)
        //res.errorDF.persist(MEMORY_ONLY)

        // do the field drops first
        println("Starting all field drops...")
        res = processField(
          listOldNew.filter( on => (on.oldField.nonEmpty && on.newAFC.isEmpty )),
          res )

        // then the field additions
        println("Starting all field additions...")
        res = processField(
          listOldNew.filter( on => (on.oldField.isEmpty && on.newAFC.nonEmpty )),
          res )

        // do all the changes last
        println("Starting all (potential) field changes...")
        res = processField(
          listOldNew.filter( on => (on.oldField.nonEmpty && on.newAFC.nonEmpty )),
          res )

        println("Pulling out all fields where the erroField has something (converting errorFieldSplit to all strings)...")
        println("!!!!!!!! res.count = "+res.count)
        res.show
        val errorFieldSplit = res.split( col(errorField).isNull || length(trim(col(errorField))) === lit(0) )
        val goodDF = errorFieldSplit.positive.drop(errorField)
        val errDF = errorFieldSplit.negative.convertToAllStrings
        println("!!!!!!!! goodDF.count = "+goodDF.count)
        println("!!!!!!!! errDF.count = "+errDF.count)

        DataFrameOpResult(goodDF, errDF)
      }
      println("changeSchema: DONE")
      result
    }

    /** After calling changeSchema on a dataframe, you could call this to 
      * add any errors to a field you specify.
      */
    def addChangeSchemaErrorsToRejectReasonField(
      rejectReasonField: String, 
      separator: String): 
      DataFrame = {

      def hasRejectReasonField(df: DataFrame): Boolean = 
        df.schema.fields.find( _.name == rejectReasonField ).nonEmpty

      // add the error field to the reasonforreject field
      if ( !hasRejectReasonField(df) ) {
        // don't have it yet, just rename the error field to be reject reason
        df.withColumnRenamed(changeSchemaErrorField, rejectReasonField)
      }
      else {
        // have it already, need to update the field:
        df.withColumn(
          rejectReasonField,
          concat_ws( separator, col(rejectReasonField), col(changeSchemaErrorField))
        )
        .drop(changeSchemaErrorField)
      }
    }


    /** Reorder columns to match something else.  Required before doing unionAll()
      * in Spark 1.5.
      */
    def reorderColumns(seq: Seq[String]): DataFrame = {

      // compare sets of columns, they must match
      val requestedSet = seq.toSet
      val thisSet = df.schema.fields.map(_.name).toSet
      if (requestedSet != thisSet) throw new Exception("DataFrameUtil.reorderColumns: new column set must match content and size of existing column set.")

      df.select(seq.head, seq.tail: _*)
      //df.select(seq)
    }

    /** Do a safe unionall that reorders columns to match before unioning.
      */
    def safeUnionAll(other: DataFrame): DataFrame = {
      // reorder 'other' schema to match this one.
      val reorderedOther = other.reorderColumns(df.schema.fields.map(_.name).toSeq)
      // Now do the union
      df.unionAll(reorderedOther)
    }

    /** "Flatten" a dataframe so all the fields in StructTypes are moved to the
      * top level.  
      * 
      * Note that spark removes the prefixes even though this doesn't look like 
      * it would.
      */
    def flatten: DataFrame = {

      def flattenSchema(schema: StructType, prefix: Option[String] = None):
        Array[Column] = {

        schema.fields.flatMap{structField => {
          val colName = {
            if (prefix.isEmpty) structField.name
            else prefix.get + "." + structField.name
          }
          structField.dataType match {
            case st: StructType => flattenSchema(st, Option(colName))
            case _ => Array(col(colName))
          }
        }}
      }
      df.select(flattenSchema(df.schema):_*)
    }


    /** Action that does a cheap take & count to determine if this 
      * dataframe is empty or not.
      */
    def isEmpty(): Boolean = {
      df.take(1).size == 0
    }
  }
}
