package com.cars.turbocow

import com.cars.turbocow.actions.ActionList

case class Item(
  override val actions: List[Action] = List.empty[Action],
  name: Option[String] = None
) extends ActionList

