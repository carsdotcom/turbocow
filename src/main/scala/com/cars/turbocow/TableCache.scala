package com.cars.turbocow


/** Abstraction to do a lookup into a cached table.
  * 
  */
trait TableCache extends Serializable {

  /** Do a lookup.  
    * 
    * @param keyField - name of the key field to search on
    * @param keyValue - the value to find in the key field (the value of the 
    *                   "primary key")
    * @param select - list of fields whose values should be grabbed from the row
    * 
    * @return key-values of everything requested in the select List.  
    *         Return None if the key is not found.
    */
  def lookup(
    keyField: String,
    keyValue: Any,
    select: List[String]
  ): Option[Map[String, Option[String]]]

}


/** Some helper functions related to caching.
  */      
object TableCache {

  /** Scan a list of Items and return a map of dbAndTable to list of 
    * all Lookup actions on that table.
    * 
    * @param  items list to search through for actions needing cached lookup tables
    * @return list of CachedLookupRequirements objects, one per table
    */
  def getAllLookupRequirements(items: List[Item]):
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
