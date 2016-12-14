package com.cars.bigdata.turbocow

import org.apache.spark.sql.types.{StringType, StructType}

object SchemaUtil {

  /** Conversion helper function
    */
  def convertToAllStringStructType(fields: Seq[AvroFieldConfig]): StructType = {
    StructType(
      fields = convertToAllString(fields).map( _.structField ).toArray
    )
  }

  /** Conversion helper function
    */
  def convertToAllString(fields: Seq[AvroFieldConfig]): Seq[AvroFieldConfig] = {
    fields.map( afc =>
      afc.copy(structField = afc.structField.copy(dataType=StringType))
    )
  }
}

