package com.cars.ingestionframework

import Defs._
import scala.annotation.tailrec

object MapUtil
{
  /** Merge two maps.  There must be an easier way to do this. (TODO)
    * NOTE that the "additionMap" is merged into the existingMap. 
    * Any existing keys are overwritten.
    */
  def merge(additionMap: StringMap, existingMap: StringMap): StringMap = {
    //@tailrec
    //def recursiveMerge(remainingMap: StringMap, accumulatedMap: StringMap): StringMap = {
    //  val headOpt = remainingMap.headOption
    //  if(headOpt.isEmpty)
    //    accumulatedMap
    //  else 
    //    recursiveMerge(remainingMap.tail, accumulatedMap + headOpt.get)
    //}
    //recursiveMerge(additionMap, existingMap)
    existingMap ++ additionMap // there you go, there's the easier way
  }
}

