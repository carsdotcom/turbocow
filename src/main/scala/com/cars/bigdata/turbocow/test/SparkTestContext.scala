package com.cars.bigdata.turbocow.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

import java.util.Date

// This class provides a global spark context, sqlCtx, and hiveCtx for use
// in tests.  TODO - call close() after ALL tests.
object SparkTestContext
{

  // create the conf (borrowed from spark-testing-base)
  val appID = new Date().toString + math.floor(math.random * 10E4).toLong.toString
  lazy val _conf = new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID)
  def conf = _conf

  // spark context
  lazy val _sc = new SparkContext(_conf)
  def sc = _sc

  // sql and hive contexts
  lazy val _sqlCtx = new SQLContext(_sc)
  def sqlCtx = _sqlCtx

  lazy val _hiveCtx = new HiveContext(_sc)
  def hiveCtx = _hiveCtx

  // stop the spark context (only call once or don't call at all)
  // we are forking the jvm so this shouldn't be technically necessary
  def stop() = {
    _sc.stop
  }

}
