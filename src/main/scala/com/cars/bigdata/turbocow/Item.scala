package com.cars.bigdata.turbocow

import com.cars.bigdata.turbocow.actions.ActionList

case class Item(
  override val actions: List[Action] = List.empty[Action],
  name: Option[String] = None
) extends ActionList

