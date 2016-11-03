package com.cars.bigdata.turbocow

case class AvroOutputWriterConfig(

  alwaysTrimStrings: Boolean = true,

  // Always trim numerics & booleans before conversion.
  // Without it, the conversion will fail but if you are very strict with your 
  // data this may be needed.
  alwaysTrimNumerics: Boolean = true,
  alwaysTrimBooleans: Boolean = true,

  // This is the number of partitions that output dataframe will be 
  // repartitioned/coalesced to before writing out.
  // Note that lower values may run into a Spark Int.MAX_VALUE error.
  // Tune this to your liking.
  // Setting to zero disables the dataframe repartition before writing.
  numOutputPartitions: Int = AvroOutputWriterConfig.defaultNumOutputPartitions
)

object AvroOutputWriterConfig {

  // The default numOutputPartitions value.
  val defaultNumOutputPartitions = 4000
}

