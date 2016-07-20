package com.cars.bigdata.turbocow

import scala.collection.immutable.HashMap
import scala.collection.mutable.Queue

class ScratchPad extends Serializable()
{

  // main storage is a k-v map, Any values
  private var main: Map[String, Any] = new HashMap[String, Any]

  // result storage is also a k-v map but with String values
  private var results: Map[String, String] = new HashMap[String, String]

  // -----------------------------------------------------------------
  // Set / get a key of any type

  def size = main.size

  def set(key: String, value: Any) = {
    main = main + (key->value)
  }

  def get(key: String): Option[Any] = main.get(key)

  def remove(key: String): Option[Any] = {
    val removed = get(key)
    main = main - key 
    removed
  }

  // -----------------------------------------------------------------
  // Set an action result.  Give the key as the actionType (as in the json).
  // Note these will be overwritten as new results come in.

  def setResult(actionType: String, result: String) = {
    results = results + (actionType->result)
  }

  def getResult(actionType: String): Option[String] = results.get(actionType)

  def resultSize = results.size

  def removeResult(key: String): Option[String] = {
    val removed = getResult(key)
    results = results - key 
    removed
  }
}

object ScratchPad
{
//  val failureKey = "failureMessages"
}
