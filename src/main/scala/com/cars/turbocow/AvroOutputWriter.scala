package com.cars.turbocow

import java.io.Serializable
import java.lang.{Boolean, Double, Long}
import java.text.SimpleDateFormat
import java.util
import java.util.Map.Entry
import java.util.Calendar
import java.text.SimpleDateFormat
import com.cars.turbocow._
import com.cars.turbocow.actions._

import scala.collection.immutable.HashMap
import org.apache.spark.broadcast.Broadcast
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
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object AvroOutputWriter
{
  /** Output data to avro
    * 
    * @param rdd RDD to write out
    * @param outputDir the dir to write to (hdfs:// typically)
    * @param schemaPath path to schema file
    */
  def write(
    rdd: RDD[Map[String, String]], 
    outputDir: String, 
    schemaPath: String,
    sc: SparkContext ) = {

    val schema = getAvroSchema(schemaPath, sc)

    // Loop through enriched record fields
    val rowRDD = rdd.map { i =>
      val av = schema.head.map(column => i.get(column).getOrElse(null)).toList
      Row.fromSeq(av)
    }

    // create a dataframe of RDD[row] and Avro schema
    val structTypeSchema = StructType(schema(0).map(column => StructField(column, StringType, true))) // Parse AvroSchema as Instance of StructType
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.createDataFrame(rowRDD, structTypeSchema).repartition(10)

    //println("================================= dataFrame = ")
    //dataFrame.printSchema
    //dataFrame.show

    dataFrame.write.format("com.databricks.spark.avro").save(outputDir)
  }

  /** Process AvroSchema from HDFS
    *
    * @param hdfsPath
    * @param sc SparkContext
    */
  def getAvroSchema(
    hdfsPath : String, 
    sc: SparkContext): 
    Array[Array[String]] = {

    val jsonRDD = sc.textFile(hdfsPath)
    val oneLineAvroSchema = jsonRDD.collect().mkString("")
    val lineRDD = sc.parallelize(List(oneLineAvroSchema))
    val parseJsonRDD = lineRDD.map { record =>
      implicit val jsonFormats = org.json4s.DefaultFormats
      parse(record)
    }
    val fieldsList = parseJsonRDD.collect().map(eachline => {

      implicit val formats = org.json4s.DefaultFormats

      //collect fields array from avro schema
      val fieldsArray = (eachline \ "fields").children

      //make array from all field names from avro schema
      val b = fieldsArray.map(eachChild =>
        (eachChild \ "name").extract[String])
      b.toArray
    })
    fieldsList
  }
}

