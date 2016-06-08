package com.cars.ingestionframework

import org.apache.spark.sql.hive.HiveContext
import scala.util.Try
import org.apache.spark.sql.Row

/** Class to encompass a cached dataframe in memory.  Stored as a key -> Row
  * map.  Key can be any type (it must be supported by hive though).
  * 
  */
class HiveTableCache(
  tableMap: Map[String, Map[Any, Row]] )
  extends TableCache {

  // check the table.  This is normally not how to construct this.  Use the 
  // companion object apply() instead.
  tableMap.headOption.getOrElse(throw new Exception("tableMap must not be empty!"))

  /** Do a lookup.
    * 
    */
  override def lookup(
    lookupField: String,
    lookupValue: Any,
    select: String // field to select
  ): Option[String] = {

    //if(lookupField != keyField) throw new Exception(s"lookupField($lookupField) is not equal to keyField($keyField)")

    // todo log this error instead?  Below, on error, we just return None
    val map = tableMap.getOrElse(lookupField, throw new Exception(s"""couldn't find HiveTableCache indexed on field: "$lookupField)"""))

    val rowOpt = map.get(lookupValue)
    //println("RRRRRRRRRRRRRRRR lookupValue = "+lookupValue)
    //tableMap.foreach{ case (k, v) => println(s"RRRRRR key($k), value($v)") }
    //println("RRRRRRRRRRRRRRRR rowOpt = "+rowOpt)

    val foundValue = 
      if (rowOpt.isEmpty) None
      else {
        Try(rowOpt.get.getAs[String](select).trim).toOption
      }
    //println("FFFFFFFFFFFFFFFF foundValue = "+foundValue)
    foundValue
  }
}

/** Companion object.  Use this constructor so that we don't store the hiveContext
  * in the object.  (It gets broadcast by spark)
  */
object HiveTableCache
{
  /** Alternate constructor
    */
  def apply(
    hiveContext: Option[HiveContext],
    databaseName: Option[String],
    tableName: Option[String],
    keyFields: List[String],
    fieldsToSelect: List[String]
  ): HiveTableCache = {

    // check all the input
    hiveContext.getOrElse(throw new Exception("hiveContext was null!"))
    ValidString(databaseName).getOrElse(throw new Exception("databaseName was not a valid string!"))
    ValidString(tableName).getOrElse(throw new Exception("tableName was not a valid string!"))
    keyFields.foreach{ kf => ValidString(kf).getOrElse(throw new Exception("a keyField was not a valid string!: "+keyFields)) }
    fieldsToSelect.headOption.getOrElse(throw new Exception("fieldsToSelect must not be empty!"))
    fieldsToSelect.foreach{ f => ValidString(f).getOrElse(throw new Exception("a fieldToSelect was not a valid string!: "+fieldsToSelect)) }

    // create the dataframe.
    val fields = (keyFields ++ fieldsToSelect).distinct.mkString(",")
    val query = s"""
      SELECT $fields
        FROM ${databaseName.get}.${tableName.get}
    """
    val df = hiveContext.get.sql(query)
    //println("SSSSSSSSSSSSSSSSSSS showing df:")
    //df.show
    //println("SSSSSSSSSSSSSSSSSSS showed df.")

    // first collect
    val allRows = df.collect

    // then get a reference map which we will refer to later
    val refMap: Map[Any, Row] = allRows.map{ row =>
      (row.getAs[Any](keyFields.head) -> row)
    }.toMap

    // create the other maps, using the reference map (use tail - skipping the head)
    val otherMaps: Map[String, Map[Any, Row]] = keyFields.tail.flatMap{ keyField => 
      Some(
        keyField, 
        refMap.map{ case (refKey, refRow) => 
          (refRow.getAs[Any](keyField) -> refRow)
        }
      )
    }.toMap

    // return otherMaps with the addition of the refmap
    val tableMap = otherMaps + (keyFields.head-> refMap)

    new HiveTableCache(tableMap)
  }
}


// usage test - stick in main somewhere, run on cluster
/*

  def check(boolCheck: Boolean, errorMessage: String = "<no message>" ) = {
    if( ! boolCheck ) throw new RuntimeException("ERROR - Check failed:  "+errorMessage)
  }
  def checkEqual(left: String, right: String, errorMessage: String = "<no message>" ) = {
    if( left != right ) throw new RuntimeException(s"ERROR - Check failed:  left($left) not equal to right($right):  $errorMessage")
  }

      val db = "dw_dev"
      val affiliateTable = "affiliate"
      val caches: Map[String, TableCache] = Map("affiliate"-> HiveTableCache(
        hiveContext,
        db, 
        tableName = "affiliate", 
        keyField = "ods_affiliate_id",
        fieldsToSelect = List("affiliate_id"))
      )

      //SELECT ods_affiliate_id, affiliate_id from affiliate
      // gives: 
      // ods aff id(int)  aff id (str)
      // 9301129          9301129O
      // 7145092          7145092O   
      // 8944533          8944533O
      // note:
      // `affiliate_id` varchar(11), 
      // `ods_affiliate_id` int, 

      val keyResults = Array(
        (9301129, "9301129O"),
        (7145092, "7145092O"),
        (8944533, "8944533O")).toSeq

      checkEqual( caches("affiliate").lookup("ods_affiliate_id", keyResults(0)._1, select="affiliate_id").get, keyResults(0)._2, "test 1 failed")
      checkEqual( caches("affiliate").lookup("ods_affiliate_id", keyResults(1)._1, select="affiliate_id").get, keyResults(1)._2, "test 2 failed")
      checkEqual( caches("affiliate").lookup("ods_affiliate_id", keyResults(2)._1, select="affiliate_id").get, keyResults(2)._2, "test 3 failed")

      // test the caches broadcast
      val tableCaches = sc.broadcast(caches)
      val numRDD = sc.parallelize(keyResults)
      val resultRDD = numRDD.map{ case (key, result) => 
        (key, tableCaches.value("affiliate").lookup("ods_affiliate_id", key, select="affiliate_id")) // returns option
      }

      val krMap = keyResults.toMap
      resultRDD.collect.foreach{ result => 
        val resultVal = result._2
        check(resultVal.nonEmpty)
        checkEqual(resultVal.get, krMap(result._1))
      }

      // DONE
      println("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE")
      println("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE")
      println("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE")
      throw new Exception("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE (The end)")
*/


