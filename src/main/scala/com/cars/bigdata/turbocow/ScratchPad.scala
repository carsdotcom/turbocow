package com.cars.bigdata.turbocow

import scala.collection.immutable.HashMap
import scala.collection.mutable.Queue

class ScratchPad extends Serializable
{

  // main storage is a k-v map, Any values
  private var mainPad: Map[String, Any] = new HashMap[String, Any]
  def allMainPad = mainPad

  // result storage is also a k-v map but with String values
  private var results: Map[String, String] = new HashMap[String, String]
  def allResults = results

  // -----------------------------------------------------------------
  // Set / get a key of any type

  def size = mainPad.size

  def set(key: String, value: Any) = {
    mainPad = mainPad + (key->value)
  }

  def get(key: String): Option[Any] = mainPad.get(key)

  def remove(key: String): Option[Any] = {
    val removed = get(key)
    mainPad = mainPad - key 
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
