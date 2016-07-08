package com.cars.turbocow


/** Abstraction to do a lookup into a cached table.
  * 
  */
trait TableCache extends Serializable {

  /** Do a lookup.  
    * 
    * @param keyField - name of the key field to search on
    * @param keyValue - the value to find in the key field (the value of the 
    *                   "primary key")
    * @param select - list of fields whose values should be grabbed from the row
    * 
    * @return key-values of everything requested in the select List.  
    *         Return None if the key is not found.
    */
  def lookup(
    keyField: String,
    keyValue: Any,
    select: List[String]
  ): Option[Map[String, Option[String]]]

}

