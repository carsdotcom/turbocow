package com.cars.turbocow

import org.json4s.JValue

/** ActionCreator - creates all of the SourceActions based on the config file.
  *
  */
trait ActionCreator {

  /** Create an Action object based on the actionType and config from the json.
    * Note: config will be JNothing if not present.
    * Called from the ActionFactory.create(), above.
    *
    * @return Some[Action] if able to handle this actionType; None if this 
    *         actionType is unknown to this creator.
    */
  def createAction(
    actionType: String, 
    actionConfig: JValue, 
    rejectionReason: Option[RejectionReason] = None): 
    Option[Action]

}


