package com.cars.bigdata.turbocow

import org.apache.spark.sql.Row

import scala.util.Try

/** DataFrame helper functions.
  */
object RowUtil
{
  implicit class RowAdditions(val row: Row) {

    /** Returns true if a field is null, or the column is missing.
      */
    def fieldIsNull(fieldName: String): Boolean = {
      val index = Try{ row.fieldIndex(fieldName) }
      ( index.isFailure || 
        index.get < 0 || 
        index.get >= row.size || 
        row.isNullAt(index.get) )
    }
  }
}
