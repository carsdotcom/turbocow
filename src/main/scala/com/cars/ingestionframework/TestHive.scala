package com.cars.ingestionframework.exampleapp

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by nchaturvedula on 5/18/2016.
  * 
  * @todo Add real test, remove this.
  */
object TestHive extends Serializable {

  def main(args: Array[String]): Unit =
  {
    val sparkConf = new SparkConf().setAppName("Test Hive").setMaster("yarn-client")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    def testDataFrame(
      testNum: Int, 
      query: String, 
      whereClause: Option[String] = None) = {

      println(s"$$$$$$$$$$$$$$$$$$$$$$$$$$ testNum: $testNum:  creating dataFrame....")
      val dataFrame = 
        if(whereClause.isEmpty) 
          hiveContext.sql(query)
        else {
          hiveContext.sql(query).where(whereClause.get)
        }
      println(s"$$$$$$$$$$$$$$$$$$$$$$$$$$ testNum: $testNum:  dataFrame created.")
      println(s"$$$$$$$$$$$$$$$$$$$$$$$$$$ testNum: $testNum:  dataFrame show...")
      dataFrame.show()
      println(s"$$$$$$$$$$$$$$$$$$$$$$$$$$ testNum: $testNum:  dataFrame show done.")
    }

    testDataFrame(1, s"""select zip_code from dw_dev.zip_code where zip_code_id='06912A'""")

    testDataFrame(2, "select zip_code from dw_dev.zip_code", Some("zip_code_id='06912A'"))

    //val dataFrame1 = hiveContext.sql("select reviewer from als_search.testing_v2")
    //  .where("reviewer='nageswar'")
    //
    //println("$$$$$$$$$$$$$$$$$$$$$$$$$$ dataFrame1 show:")
    //dataFrame1.show()
    //println("$$$$$$$$$$$$$$$$$$$$$$$$$$ dataFrame1 show done")
    //
    //val dataFrame2 = hiveContext.sql("select cpo_program_id from dw_dev.vehicle_test")
    //  .where("vehicle_id='-88839221O'")
    //println("$$$$$$$$$$$$$$$$$$$$$$$$$$ dataFrame2 show:")
    //dataFrame2.show()
    //println("$$$$$$$$$$$$$$$$$$$$$$$$$$ dataFrame2 show done")

    //val dataFrame31 = hiveContext.sql(s"""select zip_code from dw_dev.zip_code where zip_code_id='06912A'""")
    //println("$$$$$$$$$$$$$$$$$$$$$$$$$$ dataFrame31 show:")
    //dataFrame31.show()
    //println("$$$$$$$$$$$$$$$$$$$$$$$$$$ dataFrame31 show done")
    //
    //val dataFrame3 = hiveContext.sql("select zip_code from dw_dev.zip_code").where ("zip_code_id='06912A'")
    //println("$$$$$$$$$$$$$$$$$$$$$$$$$$ dataFrame3 show:")
    //dataFrame3.show()
    //println("$$$$$$$$$$$$$$$$$$$$$$$$$$ dataFrame3 show done")
    //
    //val dataFrame4 = hiveContext.sql("select vehicle_id,legacy_vehicle_id,ods_vehicle_id from dw_dev.test12").where ("ods_vehicle_id='46589078'")
    //println("$$$$$$$$$$$$$$$$$$$$$$$$$$ dataFrame4 show:")
    //dataFrame4.show()
    //println("$$$$$$$$$$$$$$$$$$$$$$$$$$ dataFrame4 show done")
    //
    //val dataFrame5 = hiveContext.sql("select vehicle_id from dw_dev.vehicle ").where (" vehicle_id='-88839221O'")
    //println("$$$$$$$$$$$$$$$$$$$$$$$$$$ dataFrame5 show")
    //dataFrame5.show()
    //println("$$$$$$$$$$$$$$$$$$$$$$$$$$ dataFrame5 show done")

  }

}
