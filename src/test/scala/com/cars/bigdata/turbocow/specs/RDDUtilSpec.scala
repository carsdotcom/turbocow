package com.cars.bigdata.turbocow

import com.cars.bigdata.turbocow.test.SparkTestContext._

import RowUtil._

class RDDUtilSpec extends UnitSpec {

  describe("split()")  // ------------------------------------------------
  {
    it("should split an RDD into two using same filter") {
      
      val rdd = sc.parallelize(List(10, 11, 12, 13, 14))

      val (posRDD, negRDD) = RDDUtil.split(rdd, (e: Int) => (e % 2) == 0)

      val posCollected = posRDD.collect()
      posCollected.size should be (3)
      posCollected.foreach{ e =>
        (e % 2) should be (0)
      }

      val negCollected = negRDD.collect()
      negCollected.size should be (2)
      negCollected.foreach{ e =>
        (e % 2) should be (1)
      }
    }
  }

  describe("convertEnrichedRDDToDataFrameForFurtherProcessing()") {
    
    val eRdd = sc.parallelize(List(
      Map("A"-> "A1", "B"->"B1"),
      Map("B"->"B2", "C"-> "C2")
    ))

    val df = RDDUtil.convertEnrichedRDDToDataFrameForFurtherProcessing(eRdd, sqlCtx)

    it("should add fields that exist in all records") {
      df.schema.fields.size should be (3)
      df.schema.fields.map( _.name ).toSeq.sorted should be (Seq("A", "B", "C"))
    }

    it("should null any fields for which values don't exist in a record but do in others") {
      val rows = df.collect
      rows.foreach{ row => row.getAs[String]("B") match {
        case "B1" => 
          row.getAs[String]("A") should be ("A1")
          row.fieldIsNull("C") should be (true)
        case "B2" =>
          row.fieldIsNull("A") should be (true)
          row.getAs[String]("C") should be ("C2")
        case _ => fail
      }}
    }
  }
}





