package com.cars.ingestionframework


/** Abstraction to do a lookup into a cached table.
  * 
  */
trait TableCache extends Serializable {

  /** Do a lookup.  Returns a Some, or None if the key is not found.
    */
  def lookup(
    lookupField: String,
    lookupValue: Any,
    select: String // fields to select
  ): Option[String] = None

}

