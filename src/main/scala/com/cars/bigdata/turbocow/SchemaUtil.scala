package com.cars.bigdata.turbocow

import org.apache.spark.sql.types.{StringType, StructType}

object SchemaUtil {

  /** Conversion helper function
    */
  def convertToAllStringStructType(fields: Seq[AvroFieldConfig]): StructType = {
    StructType(
      fields = fields.map(
        _.structField.copy(dataType=StringType)
      ).toArray
    )
  }
}

