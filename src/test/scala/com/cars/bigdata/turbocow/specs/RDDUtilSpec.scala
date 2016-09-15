package com.cars.bigdata.turbocow

import com.cars.bigdata.turbocow.test.SparkTestContext._

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
}





