package com.cars.bigdata.turbocow

/** Package object to add some helpful utils in scope by just importing utils._
  */
package object utils {

  implicit class StringAdditions(val s: String) {

    // Add ltrim & rtrim methods to String
    def ltrim = s.replaceAll("^\\s+", "")
    def rtrim = s.replaceAll("\\s+$", "")
  }

}

