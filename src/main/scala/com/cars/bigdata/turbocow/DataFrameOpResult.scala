package com.cars.bigdata.turbocow

import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.DataFrame

case class DataFrameOpResult(
  goodDF: DataFrame, 
  errorDF: DataFrame) {

  def persist(storageLevel: StorageLevel) = {
    goodDF.persist(storageLevel)
    errorDF.persist(storageLevel)
  }

  def unpersist(blocking: Boolean = false) = {
    goodDF.unpersist(blocking)
    errorDF.unpersist(blocking)
  }
}


