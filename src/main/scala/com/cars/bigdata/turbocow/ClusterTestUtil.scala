package com.cars.bigdata.turbocow

object ClusterTestUtil
{

  /** Check an assumption
    */
  def checkFunction(boolCheck: Boolean, errorMessage: String = "<no message>", throwOnFail: Boolean = true ): Boolean = {
    val msg = "ERROR - Check failed:  "+errorMessage
    if( ! boolCheck ) {
      if (throwOnFail) throw new RuntimeException(msg)
      else println(msg)
    }
    true
  }

  /** Check if two things are equal
    */
  def checkEqualFunction(left: String, right: String, errorMessage: String = "<no message>", throwOnFail: Boolean = true ): Boolean = {
    val msg = s"ERROR - Check failed:  left($left) not equal to right($right):  $errorMessage"
    if( left != right )  {
      if (throwOnFail) throw new RuntimeException(msg)
      else println(msg)
    }
    true
  }

  def checkThrow(boolCheck: Boolean, errorMessage: String = "<no message>" ) = 
    checkFunction(boolCheck, errorMessage, throwOnFail = true)

  def checkEqualThrow(left: String, right: String, errorMessage: String = "<no message>" ) = 
    checkEqualFunction(left, right, errorMessage, throwOnFail = true)

  def checkPrint(boolCheck: Boolean, errorMessage: String = "<no message>" ) =
    checkFunction(boolCheck, errorMessage, throwOnFail = false)

  def checkEqualPrint(left: String, right: String, errorMessage: String = "<no message>" ) = 
    checkEqualFunction(left, right, errorMessage, throwOnFail = false)


  /** Check if two optional things are equal and nonEmpty
    */
  def checkEqualPrintOpt(left: Option[String], right: Option[String], errorMessage: String = "<no message>" ) = {
    if ( (left.isEmpty || right.isEmpty) ) {

      val sb = new StringBuilder
      sb.append(s"ERROR - Check failed in checkEqualPrint:  ")
      if(left.isEmpty) sb.append("left is None; ")
      if(right.isEmpty) sb.append("right is None.  ")
      sb.append("\n  Stacktrace:  \n" + new Throwable().getStackTrace().take(5).mkString("    ", "\n    ", ""))
      println(sb.toString)
    }
    else
      checkEqualFunction(left.get, right.get, errorMessage, throwOnFail = false)
  }

}
