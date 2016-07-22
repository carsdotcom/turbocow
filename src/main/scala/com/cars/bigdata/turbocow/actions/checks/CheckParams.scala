package com.cars.bigdata.turbocow.actions

case class CheckParams(
  val left: String,
  val right: Option[String] = None
//  val leftSource: Option[String] = None,
//  val rightSource: Option[String] = None
) extends Serializable

object CheckParams {

  def fromUnaryCheck(uc: UnaryCheck): CheckParams = CheckParams(
    uc.field
  )

/*
  def fromBinaryCheck(bc: BinaryCheck): CheckParams = CheckParams(
    bc.left,
    Option(bc.right)
  )
  */
}




