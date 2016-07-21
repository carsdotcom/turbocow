package com.cars.bigdata.turbocow

import org.scalatest.{Matchers, FunSpec}

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.mock.MockitoSugar

abstract class UnitSpec 
  extends FunSpec 
  with Matchers 
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with MockitoSugar


