package com.cars.bigdata.turbocow

import org.apache.spark.storage.StorageLevel

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
  numOutputPartitions: Int = AvroOutputWriterConfig.defaultNumOutputPartitions,

  // If this is a Some, the AvroOutputWriter will persist the dataframe 
  // in the manner specified before writing.  TODO consider DISK_ONLY as default after testing.
  persistence: Option[StorageLevel] = None
)

object AvroOutputWriterConfig {

  // The default numOutputPartitions value.
  val defaultNumOutputPartitions = 4000
}

