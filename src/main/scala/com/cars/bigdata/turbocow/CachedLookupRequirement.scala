package com.cars.bigdata.turbocow

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
    CachedLookupRequirement(dbTableName, allKeyFields(other), allNeededFields(other), jsonRecordsFile)
  }

}

object CachedLookupRequirement {

  /** Scan a list of Items and return a map of dbAndTable to list of 
    * all Lookup actions on that table.
    * 
    * @param  items list to search through for actions needing cached lookup tables
    * @return list of CachedLookupRequirements objects, one per table
    */
  def getAllFrom(items: List[Item]):
    List[CachedLookupRequirement] = {

    val allReqList: List[ (String, CachedLookupRequirement) ] = items.flatMap{ item =>
      val actionReqs: List[CachedLookupRequirement] = item.actions.flatMap{ action =>
        action.getLookupRequirements
      }
      actionReqs
    }.map{ req => (req.dbTableName, req) }

    val allReqsMap: Map[ String, List[CachedLookupRequirement]] = 
      allReqList.groupBy{ _._1 }.map{ case(k, list) => (k, list.map{ _._2 } ) }

    // combine to form one map of dbTableName to requirements.
    // I feel like this last bit could be simplified.  TODO
    val combinedRequirements: Map[String, CachedLookupRequirement] = allReqsMap.map{ case (dbTableName, reqList) =>
      ( dbTableName,
        reqList.reduceLeft{ (combined, e) =>
          combined.combineWith(e)
        } 
      )
    }

    combinedRequirements.toList.map{ _._2 }
  }


}

