package com.cars.ingestionframework

trait TableCache extends Serializable {

  /** Do a lookup.  Returns a Some, or None if the key is not found.
    */
  def lookup(
    lookupField: String,
    lookupValue: Any,
    select: String // fields to select
  ): Option[String] = None

}

