package com.cars.ingestionframework

import org.apache.spark.sql.hive.HiveContext

case class ActionContext(
  hc: Option[HiveContext]
)

