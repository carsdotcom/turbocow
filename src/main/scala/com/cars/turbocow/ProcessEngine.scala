package com.cars.turbocow.exampleapp

import java.lang.{Boolean, Double, Long}
import java.util
import java.util.Map.Entry
import java.util.concurrent.TimeUnit

import com.cars.turbocow._

import scala.collection.immutable.HashMap
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.json4s._
import org.json4s.jackson.JsonMethods._
import Defs._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.hive.HiveContext
import com.typesafe.config._

class ProcessEngine {

  // todo - move enrich() here, refactor?

}
