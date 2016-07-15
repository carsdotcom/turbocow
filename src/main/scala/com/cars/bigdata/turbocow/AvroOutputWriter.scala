package com.cars.bigdata.turbocow

import java.io.Serializable
import java.lang.{Boolean, Double, Long}
import java.text.SimpleDateFormat
import java.util
import java.util.Map.Entry
import java.util.Calendar
import java.text.SimpleDateFormat
import com.cars.bigdata.turbocow._
import com.cars.bigdata.turbocow.actions._

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
  /** Output data to avro using a specific Avro schema file.
    * 
    * @param rdd RDD to write out
    * @param outputDir the dir to write to (hdfs:// typically)
    * @param schemaPath path to schema file
    * @param sc spark context
    */
  def write(
    rdd: RDD[Map[String, String]], 
    schemaPath: String,
    outputDir: String, 
    sc: SparkContext ): 
    Unit = {

    // get the list of field names from avro schema
    val schema: List[String] = getAvroSchema(schemaPath, sc)

    write(rdd, schema, outputDir, sc)
  }

  /** Output data to avro - with list of fields for the schema (useful for testing)
    * 
    * @param rdd RDD to write out
    * @param outputDir the dir to write to (hdfs:// typically)
    * @param schema list of fields to write
    * @param sc spark context
    */
  def write(
    rdd: RDD[Map[String, String]], 
    schema: List[String],
    outputDir: String, 
    sc: SparkContext ):
    Unit = {

    // Loop through enriched record fields, and extract the value of each field 
    // in the order of schema list (so the order matches the Avro schema).
    val rowRDD = rdd.map { i =>
      val av: List[String] = schema.map(column => i.get(column).getOrElse(null))
      Row.fromSeq(av)
    }

    // create a dataframe of RDD[row] and Avro schema
    val structTypeSchema = StructType(schema.map(column => StructField(column, StringType, true))) // Parse AvroSchema as Instance of StructType
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
    List[String] = {

    val jsonSchema = sc.textFile(hdfsPath).collect().mkString("")
    val parsedSchema = parse(jsonSchema)

    implicit val formats = org.json4s.DefaultFormats

    // collect fields list from avro schema
    val fields = (parsedSchema \ "fields").children

    // extract just the names
    fields.map{ eachChild => (eachChild \ "name").extract[String] }
  }
}

