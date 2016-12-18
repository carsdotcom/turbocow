package com.cars.bigdata.turbocow

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object RDDUtil {

  /** Convert Enriched RDD To DataFrame For Further Processing
    * 
    * This converts the RDD to a DataFrame of all nullable strings.
    * It analyzes the RDD maps and constructs a schema of the union of all
    * possible fields.  
    * 
    * @todo support different datatypes.  Currently only strings are supported
    *       in RDD processing.
    */
  def convertEnrichedRDDToDataFrameForFurtherProcessing(
    enrichedRDD: RDD[Map[String, String]],
    sqlCtx: SQLContext): 
    DataFrame = {

    // Find all the fields in each record.

    // Merge all of the added input fields from each record, get a master 
    // set of added fields.  
    println("RDDUtil.convertEnrichedRDDToDataFrameForFurtherProcessing starting...")
    println("Calculating allAddedFields...")
    val allAddedFields = enrichedRDD.map{ record =>
      val list = record.get( ActionEngine.addedInputFieldsMarker )
      if (list.nonEmpty) 
        list.get.split(",").toSet
      else 
        Set.empty[String] 
    }.fold(Set.empty[String]){ _ ++ _ }
    println("Done calculating allAddedFields.")

    // Figure out the set of all fields already in the enriched 
    // (enriched should include input at this point)
    println("Calculating allFieldsSet...")
    val allFieldsSet = enrichedRDD.map{ record =>
      record.keySet
    }.fold(allAddedFields){ _ ++ _ }
    println("Done calculating allFieldsSet.")

    // Convert to a structtype with String as the type plus nullable
    val schema = StructType( allFieldsSet.toArray.map{ fieldName => 
      StructField(fieldName, StringType, nullable=true)
    })

    // Convert RDD to DataFrame.
    println("Converting RDD to DataFrame...")
    val rowRDD: RDD[Row] = enrichedRDD.map{ record =>
      val vals: Seq[Any] = schema.fields.map{ structField =>
        record.getOrElse(structField.name, null)
      }
      Row.fromSeq(vals)
    }
    val df = sqlCtx.createDataFrame(rowRDD, schema)
    println("RDDUtil.convertEnrichedRDDToDataFrameForFurtherProcessing done.")
    df
  }

  // todo move into additions
  def split[T](rdd: RDD[T], filter: (T) => Boolean): (RDD[T], RDD[T]) = {
    ( rdd.filter(filter), rdd.filter( (t: T) => !filter(t) ) )
  }

  implicit class RDDAdditions[T](val rdd: RDD[T]) {

    /** Helper function to quickly split an RDD into two by calling filter() twice.
      *
      * @return tuple of (positiveFilteredRDD, negativeFilteredRDD)
      */
    def split(filter: (T) => Boolean): (RDD[T], RDD[T]) = RDDUtil.split(rdd, filter)
  }


}


