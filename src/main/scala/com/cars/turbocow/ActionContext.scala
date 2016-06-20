package com.cars.turbocow

/** Class holding anything that needs to be given to Action's Perform method.
  * It is the context in which an action is performed.
  */
case class ActionContext(

  // table caches:  Dataframe Rows converted into a Map for ease of lookup
  tableCaches: Map[String, TableCache],

  // The collected list of all rejection reasons determined while processing an action
  rejectionReasons: RejectionCollection = new RejectionCollection(),

  // The 'scratchpad' - allows caching of values between actions
  scratchPad: ScratchPad = new ScratchPad
)

