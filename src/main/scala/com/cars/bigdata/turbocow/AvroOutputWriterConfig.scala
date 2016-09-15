package com.cars.bigdata.turbocow

case class AvroOutputWriterConfig(

  alwaysTrimStrings: Boolean = true,

  // Always trim numerics & booleans before conversion.
  // Without it, the conversion will fail but if you are very strict with your 
  // data this may be needed.
  alwaysTrimNumerics: Boolean = true,
  alwaysTrimBooleans: Boolean = true
)


