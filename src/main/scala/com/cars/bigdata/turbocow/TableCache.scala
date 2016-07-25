package com.cars.bigdata.turbocow

import org.apache.spark.sql.Row


/** Abstraction to do a lookup into a cached table.
  * 
  */
trait TableCache extends Serializable {

  /** Lookup function that returns the whole row if found.  
    * 
    * @param keyField the name of the key field to lookup on
    * @param keyValue the value to search for in the keyField
    * @return Some[Row] if found; None if not.
    */
  def lookup(
    keyField: String,
    keyValue: String
  ): Option[Row]

  /** Do a lookup.  Only returns the values you select.
    * 
    * @param keyField - name of the key field to search on
    * @param keyValue - the value to find in the key field (the value of the 
    *                   "primary key")
    * @param select - list of fields whose values should be grabbed from the row
    * @return 
    *  None - if the index field is not found or if the key is invalid
    *  Some(Map[String, Option[String]) if key found.  Keys are the 'select' list items,
    *  and if the requested field exists in the lookup table, the value is a Some.
    */
  def lookup(
    keyField: String,
    keyValue: String,
    select: List[String]
  ): Option[Map[String, Option[String]]]

}


