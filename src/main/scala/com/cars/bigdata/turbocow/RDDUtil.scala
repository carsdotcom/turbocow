package com.cars.bigdata.turbocow

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object RDDUtil {

  /** convert Enriched RDD To DataFrame For Further Processing
    * 
    * Just converts the RDD to a DataFrame of all strings, nullable.
    * It analyzes the RDD maps and constructs a schema of the union of all
    * possible fields.
    */
  def convertEnrichedRDDToDataFrameForFurtherProcessing(
    enrichedRDD: RDD[Map[String, String]],
    sqlCtx: SQLContext,
    repartitionTo: Int = 0):  // 0 disables repartitioning
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

    val df = { 
      val df = sqlCtx.createDataFrame(rowRDD, schema)
      if (repartitionTo > 0 ) {
        println("RDDUtil.convertEnrichedRDDToDataFrameForFurtherProcessing: repartitionining dataFrame to: "+repartitionTo)
        // Note: using repartition() because there's no way to tell how many 
        // partitions are in the DF using public API:
        df.repartition(repartitionTo)
      }
      else df
    }

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


