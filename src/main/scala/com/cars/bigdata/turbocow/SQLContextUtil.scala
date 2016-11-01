package com.cars.bigdata.turbocow

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/** DataFrame helper functions.
  */
object SQLContextUtil
{

  // New methods for SQLContext you get when importing SQLContextUtil._ :
  implicit class SQLContextAdditions(val sqlCtx: SQLContext) {

    /** Create a DataFrame that has the specified schema but no Rows.
      */
    def createEmptyDataFrame(schema: StructType): DataFrame = {
      val sc = sqlCtx.sparkContext
      sqlCtx.createDataFrame( sc.parallelize( List.empty[Row] ), schema)
    }

  }

}
