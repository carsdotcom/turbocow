package com.cars.turbocow

import com.cars.turbocow.actions.SubActionList

// TODO rename this to "Item"
case class SourceAction(
  override val actions: List[Action] = List.empty[Action],
  name: Option[String] = None
) extends SubActionList

