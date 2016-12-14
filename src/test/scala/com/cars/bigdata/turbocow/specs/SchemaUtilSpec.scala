package com.cars.bigdata.turbocow

import org.apache.spark.sql.types._
import org.json4s._
import SchemaUtil._

class SchemaUtilSpec
  extends UnitSpec 
{
  // before all tests have run
  override def beforeAll() = {
    super.beforeAll()
  }

  // before each test has run
  override def beforeEach() = {
    super.beforeEach()
  }

  // after each test has run
  override def afterEach() = {
    super.afterEach()
  }

  // after all tests have run
  override def afterAll() = {
    super.afterAll()
  }

  //sc.setLogLevel("WARN")

  describe("convertToAllStringStructType()") {
    
    it("should convert to all strings") {

      val nullable = true
      val fields = List(
        AvroFieldConfig(StructField("a", IntegerType, nullable), JNull),
        AvroFieldConfig(StructField("b", BooleanType, nullable), JNull),
        AvroFieldConfig(StructField("c", DoubleType, nullable), JNull)
      )

      val st = convertToAllStringStructType(fields)

      st.size should be (3)
      st(0).name should be ("a")
      st(1).name should be ("b")
      st(2).name should be ("c")

      st.foreach{ sf => 
        sf.dataType should be (StringType)
        sf.nullable should be (nullable)
      }
    }
  }

}



