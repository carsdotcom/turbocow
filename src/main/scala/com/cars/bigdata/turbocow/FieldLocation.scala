package com.cars.bigdata.turbocow

// Enum that describes all the possible places to read a field or write a field:
object FieldLocation extends Enumeration {

  type FieldLocation = Value

  val Constant = Value("constant")
  val Input = Value("input")
  val Enriched = Value("enriched")
  val Scratchpad = Value("scratchpad")
  // only valid for 'input' fields.  (search enriched and if not found read from input)
  val EnrichedThenInput = Value("enriched-then-input") 

  // Test if a value is valid for output
  def isOutput(fl: FieldLocation.Value): Boolean = fl match {
    case Enriched | Scratchpad => true
    case _ => false
  }

  // test if a value is valid for input (the answer is yes)
  def isInput(fl: FieldLocation.Value): Boolean = true

}


