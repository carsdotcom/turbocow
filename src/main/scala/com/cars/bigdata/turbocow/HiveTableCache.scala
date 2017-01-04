package com.cars.bigdata.turbocow

import org.apache.spark.sql.hive.HiveContext
import scala.util.Try
import org.apache.spark.sql.Row

/** Class to encompass a cached dataframe in memory.  Stored as a key -> Row
  * maps.  Each map is accessed by the field name of the key to search on.
  * The key can be any type (it must be supported by hive though).
  *
  * In other words: Map[ KeyField[String], Map[ Key[Any], Row ] ]
  * 
  */
class HiveTableCache(
  val tableMap: Map[String, Map[Any, Row]] )
  extends TableCache {

  // check the table.  This is normally not how to construct this.  Use the 
  // companion object apply() instead.
  tableMap.headOption.getOrElse(throw new Exception("tableMap must not be empty!"))

  /** Lookup function that returns the whole row if found.  
    * 
    * @param keyField the name of the key object to lookup on
    * @param keyValue the value to search for in the keyField
    * 
    * @return Some[Row] if found; None if not.
    */
  override def lookup(
    keyField: String,
    keyValue: Option[Any]
  ): Option[Row] = {

    if (keyValue.nonEmpty)  {
      // If we can't find this index's map, we just return None
      val map = tableMap.getOrElse(keyField, return None)

      val convertedKeyValue: Option[Any] = 
        keyValue
        //convertToCorrectLookupType(keyField, keyValue.get)
      if (convertedKeyValue.isEmpty){
        None
      }
      else {
        map.get(convertedKeyValue.get)
      }
    }
    else None
  }

  /** Do a lookup.  
    * 
    * @return 
    *  None - if the index field is not found or if the key is invalid
    *  Some(Map[String, Option[String]) if key found.  Keys are the 'select' list items,
    *  and if the requested field exists in the lookup table, the value is a Some.
    */
  override def lookup(
    keyField: String,
    keyValue: Option[Any],
    select: List[String]
  ): Option[Map[String, Option[String]]] = {

    val row = lookup(keyField, keyValue).getOrElse(return None)

    //println("RRRRRRRRRRRRRRRR keyValue = "+keyValue)
    //tableMap.foreach{ case (k, v) => println(s"RRRRRR key($k), value($v)") }
    //println("RRRRRRRRRRRRRRRR rowOpt = "+rowOpt)

    // Return a map.  If the select field is not found, set the field in the returned 
    // map to None.
    Option(
      select.map{ field => 
        (field, Try(String.valueOf(row.getAs[String](field)).trim).toOption)
      }.toMap
    )
  }

  /** convert the type
    */
  //def convertToCorrectLookupType(keyField: String, keyValue: Any): Option[Any] = {
  //
  //  // TODO test coverage
  //  //println(s"CCCCCCCCCCCCCCCCCCCCCCCC converting keyField, keyValue($keyField, $keyValue) to.......")
  //  val lookupTable = tableMap.get(keyField).getOrElse(return Option(keyValue)) // todo handle failure
  //  lookupTable.head._1 match {
  //    case a: Long => Try{Option(keyValue.toLong)}.getOrElse( None)
  //    case a: Int => Try{Option(keyValue.toInt)}.getOrElse( None)
  //    case a: Double => Try{Option(keyValue.toDouble)}.getOrElse( None)
  //    case a: Float => Try{Option(keyValue.toFloat)}.getOrElse( None)
  //    case a: String => Option(keyValue.toString)
  //    case a => Option(a)
  //  }
  //}
}

/** Companion object.  
  */
object HiveTableCache
{

  /** Alternate constructor.
    * Use this constructor so that we don't store the hiveContext
    * in the object.  (It gets broadcast by spark)
    * 
    */
  def apply(
    hiveContext: Option[HiveContext],
    dbTableName: String,
    keyFields: List[String],
    fieldsToSelect: List[String],
    multiFieldKeys: Set[ Set[String] ]
  ): HiveTableCache = {

    // check all the input
    hiveContext.getOrElse(throw new Exception("hiveContext was null!"))
    ValidString(dbTableName).getOrElse(throw new Exception("dbTableName was not a valid string!"))
    keyFields.foreach{ kf => ValidString(kf).getOrElse(throw new Exception("a keyField was not a valid string!: "+keyFields)) }
    fieldsToSelect.headOption.getOrElse(throw new Exception("fieldsToSelect must not be empty!"))
    fieldsToSelect.foreach{ f => ValidString(f).getOrElse(throw new Exception("a fieldToSelect was not a valid string!: "+fieldsToSelect)) }
    multiFieldKeys.foreach{ set => set.foreach{ key => ValidString(key).getOrElse(throw new Exception("a multiFieldKey was not a valid (nonzero length) string!: "+multiFieldKeys.mkString)) } }

    // create the dataframe.
    val allMultiFieldKeys = multiFieldKeys.reduce( _ ++ _ )
    val fields = (keyFields ++ fieldsToSelect ++ allMultiFieldKeys.toList).distinct.mkString(",")
    val query = s"""
      SELECT $fields
        FROM ${dbTableName}
    """
    println("HiveContext.sql - query = "+query)
    val df = hiveContext.get.sql(query)
    
    //println("SSSSSSSSSSSSSSSSSSS showing df:")
    //df.show
    //println("SSSSSSSSSSSSSSSSSSS showed df.")

    // Get the single-field index map:
    val singleFieldTableMap = if (keyFields.nonEmpty) {
      //Transform into a key->Row map on the driver
      val refMap: Map[Any, Row] = df.map( row =>
        Map(row.getAs[Any](keyFields.head) -> row)
      ).reduce(_ ++ _)

      //refMap.foreach{ case(key, row) => println( "row size: "+row.size)}
      println("**************************************************************")
      println("HiveContext.sql - query = "+query)
      println("ref map size:"+refMap.size)

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
      println("table map outer size:"+tableMap.size )
      tableMap.foreach{ case(key, map) => println("for "+key+", map size is:"+map.size)}
      tableMap
    }
    else Map.empty[String, Map[Any, Row]]

    val multiFieldKeyMap = if (multiFieldKeys.nonEmpty) {
      Map.empty[String, Map[Any, Row]]

//      //Transform into a key->Row map on the driver
//      val refMap: Map[Any, Row] = df.map( row =>
//
//
//        Map(row.getAs[Any](multiFieldKeys.head) -> row)
//      ).reduce(_ ++ _)
//
//      //refMap.foreach{ case(key, row) => println( "row size: "+row.size)}
//      println("**************************************************************")
//      println("HiveContext.sql - query = "+query)
//      println("ref map size:"+refMap.size)
//
//      // create the other maps, using the reference map (use tail - skipping the head)
//      val otherMaps: Map[String, Map[Any, Row]] = keyFields.tail.flatMap{ keyField =>
//        Some(
//          keyField,
//          refMap.map{ case (refKey, refRow) =>
//            (refRow.getAs[Any](keyField) -> refRow)
//          }
//        )
//      }.toMap
//
//      // return otherMaps with the addition of the refmap
//      val tableMap = otherMaps + (keyFields.head-> refMap)
//      println("table map outer size:"+tableMap.size )
//      tableMap.foreach{ case(key, map) => println("for "+key+", map size is:"+map.size)}
//      tableMap
    }
    else Map.empty[String, Map[Any, Row]]

    val comboMap = singleFieldTableMap ++ multiFieldKeyMap

    val htc = new HiveTableCache(comboMap)
    htc
  }

  /** Alternate constructor.
    * Use this constructor so that we don't store the hiveContext
    * in the object.  (It gets broadcast by spark)
    * 
    * This version defaults multiFieldKeys to the empty set.
    */
  def apply(
    hiveContext: Option[HiveContext],
    dbTableName: String,
    keyFields: List[String],
    fieldsToSelect: List[String]
  ): HiveTableCache = 
    apply(hiveContext, dbTableName, keyFields, fieldsToSelect, Set.empty[Set[String]])

  /** Alternate constructor - lets you specify the JSON file from which to read
    * the data and register a temporary hive table.
    * 
    * This function is meant to be used while testing.
    */
  def apply(
    hiveContext: Option[HiveContext],
    dbTableName: String,
    keyFields: List[String],
    fieldsToSelect: List[String],
    jsonRecordsFile: Option[String], // one line per record, only each line is valid json.  Can be local or hdfs, I suppose
    multiFieldKeys: Set[ Set[String] ]
  ): HiveTableCache = {

    if (jsonRecordsFile.nonEmpty) {

      // check these two vars:
      hiveContext.getOrElse(throw new Exception("hiveContext was null!"))
      ValidString(dbTableName).getOrElse(throw new Exception("dbTableName was not a valid string!"))

      // Register the temp table
      val inputDF = hiveContext.get.read.json(jsonRecordsFile.get)
      inputDF.registerTempTable(dbTableName)
    }

    // Call the other constructor
    apply(hiveContext, dbTableName, keyFields, fieldsToSelect, multiFieldKeys)
  }

  /** Alternate constructor - lets you specify the JSON file from which to read
    * the data and register a temporary hive table.
    * 
    * This function is meant to be used while testing.
    * 
    * This version defaults the multiFieldKeys to the empty set.
    */
  def apply(
    hiveContext: Option[HiveContext],
    dbTableName: String,
    keyFields: List[String],
    fieldsToSelect: List[String],
    jsonRecordsFile: Option[String] // one line per record, only each line is valid json.  Can be local or hdfs, I suppose
  ): HiveTableCache = 
    apply(hiveContext, dbTableName, keyFields, fieldsToSelect, jsonRecordsFile, Set.empty[Set[String]])

  /** Create local caches of all of the tables in the action list.
    * 
    * @return map of dbTableName to TableCache
    */
  def cacheTables(
    items: List[Item],
    hiveContext: Option[HiveContext]): 
    Map[String, TableCache] = {

    val allRequirements: List[CachedLookupRequirement] = CachedLookupRequirement.getAllFrom(items)

    // Return map of all table names to HiveTableCaches.
    allRequirements.map{ req => (
      req.dbTableName, 
      HiveTableCache(
        hiveContext,
        req.dbTableName,
        keyFields = req.keyFields,
        fieldsToSelect = req.allNeededFields,
        req.jsonRecordsFile,
        req.multiFieldKeys
      )
    )}.toMap
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


