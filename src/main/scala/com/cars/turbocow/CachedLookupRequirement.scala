package com.cars.turbocow

case class CachedLookupRequirement(
  dbTableName: String,
  keyFields: List[String] = List.empty[String],
  selectFields: List[String] = List.empty[String],
  jsonRecordsFile: Option[String] = None // optionally specify this path to create a table when preloading
)
{
  def allNeededFields: List[String] = (keyFields ++ selectFields).distinct

  def allNeededFields(other: CachedLookupRequirement): List[String] = {
    if (dbTableName != other.dbTableName) throw new Exception("combining fields from different tables!")
    (this.allNeededFields ++ other.allNeededFields).distinct
  }

  def allKeyFields(other: CachedLookupRequirement): List[String] = {
    if (dbTableName != other.dbTableName) throw new Exception("combining fields from different tables!")
    (this.keyFields ++ other.keyFields).distinct
  }

  def combineWith(other: CachedLookupRequirement): CachedLookupRequirement = {
    if (dbTableName != other.dbTableName) throw new Exception("combining fields from different tables!")
    if (jsonRecordsFile != other.jsonRecordsFile) throw new Exception("combining fields from different jsonRecordsFile(s)!")
    CachedLookupRequirement(dbTableName, allKeyFields(other), allNeededFields(other))
  }

}
