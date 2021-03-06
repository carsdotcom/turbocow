package com.cars.bigdata.turbocow

import com.cars.bigdata.turbocow.actions.checks.{BinaryCheck, UnaryCheck}
import org.json4s._
import org.json4s.jackson.JsonMethods._

/** ActionFactory - creates all of the actions to run based on the config file.
  * 
  * @param customActionCreators a list of custom ActionCreator objects to be 
  *        used, in order, before checking against the standard framework 
  *        actions.  Note that you can override the creation of standard framework 
  *        actions by indicating 
  */
class ActionFactory(val customActionCreators: List[ActionCreator] = List.empty[ActionCreator]) 
  extends ActionCreator {

  /** Alternative constructor to instantiate with just one custom creator
    */
  def this(singleActionCreator: ActionCreator) = this(List(singleActionCreator))

  /** Create the list of Item objects based on the config file.
    */
  def createItems(configJson: String): List[Item] = {

    // Parse and pass to real createItems.
    implicit val jsonFormats = org.json4s.DefaultFormats
    createItems(parse(configJson))
  }

  /** Create the list of Item objects based on the config file.
    */
  def createItems(configAST: Option[JValue]): List[Item] = {
    if (configAST.isDefined) 
      createItems(configAST.get)
    else 
      Nil
  }

  /** Create the list of Item objects based on the config file.
    */
  def createItems(configAST: JValue): List[Item] = {

    val itemsList = (configAST \ "items").children

    // transform to a list of Items:
    itemsList.map{ item =>

      // get name
      val name = JsonUtil.extractOptionString(item \ "name")
      //if (name.isEmpty) throw new Exception(s"Couldn't find 'name' string for one of the action lists.  Every action list must have a non-empty name.")

      val actions = createActionList(item \ "actions")
      if (actions.isEmpty) throw new Exception(s"""Couldn't find 'actions' array in the config file for named item "${name.getOrElse("")}".""")

      Item( actions, name )
    }
  }

  /** Process a JValue that is a JArray of Actions
    */
  def createActionList(
    actionsList: Option[JValue]): 
    List[Action] = {

    if (actionsList.isDefined)
      createActionList(actionsList.get)
    else 
      Nil
  }

  /** Process a JValue that is a JArray of Actions
    */
  def createActionList(
    actionsList: JValue): 
    List[Action] = {

    actionsList.children.map{ jval: JValue => 

      // Get the info for this action to send to the action creator.
      val actionType = JsonUtil.extractString(jval \ "actionType")
      val actionConfig = jval \ "config"

      println(s"ActionFactory:  Parsing actionType: $actionType")
      //println(s"    actionConfig: ${pretty(render(actionConfig))}")

      // First, attempt to create an action using custom creators, if any:
      val customAction: Option[Action] = if (customActionCreators.nonEmpty) {
        var action: Option[Action] = None
        val creatorIter = customActionCreators.iterator
        while (creatorIter.hasNext && action.isEmpty) {
          action = creatorIter.next.createAction(actionType, actionConfig)
        }
        action
      }
      else { 
        println("No custom action creators available.")
        None
      }

      // If a custom action was created, then use that, otherwise 
      // try the standard actions:
      customAction.getOrElse{ 
        createAction(actionType, actionConfig).getOrElse(throw new Exception(s"Couldn't create action.  Unrecogonized actionType: "+actionType))
      }
    }
  }

  /** Create an Action object based on the actionType and config from the json.
    * Note: config will be JNothing if not present.
    * Called from create(), above.
    * All standard actions supported in the framework should be created here.
    */
  override def createAction(
    actionType: String, 
    actionConfig: JValue): 
    Option[Action] = {
  
    import actions._

    actionType match {
      case "add-enriched-field" |
           "add-enriched-fields" => Option(new AddEnrichedFields(actionConfig))
      case "add-rejection-reason" => Option(new AddRejectionReason(actionConfig))
      case "add-scratch-to-enriched" => Option(new AddScratchToEnriched(actionConfig))
      case "copy" => Option(new Copy(actionConfig))
      case "check" => createCheckAction(actionConfig, Option(this))
      case JdbcLookup.actionName => Option(new JdbcLookup(actionConfig, Option(this)))
      case "lookup" => Option(Lookup(actionConfig, Option(this)))
      case "null" => Option(new NullAction(actionConfig))
      case "reject" => Option(new Reject(actionConfig))
      case "replace-null" => Option(new ReplaceNull(actionConfig))
      case "search-and-replace" => Option(new SearchAndReplace(actionConfig))
      case "simple-copy" => Option(new SimpleCopy(actionConfig))
      case _ => None
    }
  }

  def createCheckAction(actionConfig: JValue, actionFactory: Option[ActionFactory]): 
    Option[Action] = {

    val isUnary = JsonUtil.extractValidString(actionConfig \ "right").isEmpty
    isUnary match {
      case true => Option(new UnaryCheck(actionConfig, actionFactory))
      case false => Option(new BinaryCheck(actionConfig, actionFactory))
    }
  }

}


