package com.cars.bigdata.turbocow

import scala.collection.immutable.HashMap
import scala.collection.mutable.Queue

class ScratchPad(
  // main storage is a k-v map, Any values
  @volatile private var mainPad: Map[String, Any] = new HashMap[String, Any],
  // result storage is also a k-v map but with String values
  @volatile private var results: Map[String, String] = new HashMap[String, String]
)
extends Serializable
{

  def allMainPad = mainPad
  def allResults = results

  // -----------------------------------------------------------------
  // Set / get a key of any type

  def size = mainPad.size

  def set(key: String, value: Any) = {
    mainPad.synchronized {
      mainPad = mainPad + (key->value)
    }
  }

  def get(key: String): Option[Any] = mainPad.get(key)

  def remove(key: String): Option[Any] = {
    val removed = get(key)
    mainPad.synchronized {
      mainPad = mainPad - key 
    }
    removed
  }

  // -----------------------------------------------------------------
  // Set an action result.  Give the key as the actionType (as in the json).
  // Note these will be overwritten as new results come in.

  def setResult(actionType: String, result: String) = {
    results.synchronized {
      results = results + (actionType->result)
    }
  }

  def getResult(actionType: String): Option[String] = results.get(actionType)

  def resultSize = results.size

  def removeResult(key: String): Option[String] = {
    val removed = getResult(key)
    results.synchronized {
      results = results - key 
    }
    removed
  }


  /** Make a copy of this object.
    * 
    */
  def copy: ScratchPad = {
    new ScratchPad(
      new HashMap[String,Any] ++ mainPad, 
      new HashMap[String,String] ++ results
    ) 
  }
}

object ScratchPad
{
//  val failureKey = "failureMessages"
}
