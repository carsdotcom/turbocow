package com.cars.turbocow

import com.cars.turbocow.actions.ActionList

// TODO rename this to "Item"
case class SourceAction(
  override val actions: List[Action] = List.empty[Action],
  name: Option[String] = None
) extends ActionList

