package com.cars.ingestionframework

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


    val dataFrame = hiveContext.sql("select * from als_search.testing_v2")

    dataFrame.show()
  }

}
