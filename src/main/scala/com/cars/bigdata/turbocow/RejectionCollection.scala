package com.cars.bigdata.turbocow


/** A collection of rejection reasons
  * @note this is not thread safe.
  * 
  */
class RejectionCollection(
  private var reasons: List[String] = List.empty[String]
)
{
  def add(rejectionReason: String) = {
    reasons = reasons :+ rejectionReason.trim
  }

  def clear() = {
    reasons = List.empty[String]
  }

  def size = reasons.size

  def isEmpty = reasons.isEmpty
  def nonEmpty = reasons.nonEmpty

  def reasonList = reasons

  override def toString: String = {
    reasons.mkString("; ")
  }
}
