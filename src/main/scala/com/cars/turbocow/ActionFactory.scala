package com.cars.turbocow

import org.apache.spark.SparkContext
import org.json4s._
import org.json4s.jackson.JsonMethods._

/** ActionFactory - creates all of the SourceActions based on the config file.
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

  /** Create the list of SourceAction objects based on the config file.
    */

  def createSourceActions(configJson: String): List[SourceAction] = {

    implicit val jsonFormats = org.json4s.DefaultFormats

    // parse it
    val configAST = parse(configJson)

    val itemsList = (configAST \ "items").children

    // transform to a list of SourceActions:
    itemsList.map{ item =>

      // get source fields
      val sourceList = (item \ "source").toOption match {
        case Some(jval) => jval.children.map( _.values.toString)
        case _ => List.empty[String]
      }

      // get destination - is None if null, "", or missing
      val destination = JsonUtil.extractValidString(item \ "destination")

      val actions = createActionList(item \ "actions")

      SourceAction( sourceList, actions )
      // TODO: SourceAction( sourceList, destination, actions )
    }
  }

  /** Process a JValue that is a JArray of Actions
    * 
    */
  def createActionList(
    actionsList: JValue): 
    List[Action] = {

    actionsList.children.map{ jval: JValue => 

      // Get the info for this action to send to the action creator.
      val actionType = JsonUtil.extractString(jval \ "actionType")
      val actionConfig = jval \ "config"

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
  
    // regexes:
    val replaceNullWithRE = """replace-null-with-(.+)""".r
  
    actionType match {
      case "copy" => Option(new actions.Copy(actionConfig))
      case "add-enriched-fields" => Option(new AddEnrichedFields(actionConfig))
      case "lookup" => Option(actions.Lookup(actionConfig, Option(this)))
      case "reject" => Option(new actions.Reject(actionConfig))
      case replaceNullWithRE(someStr) => Option(new actions.ReplaceNullWith(someStr, actionConfig))
      case "simple-copy" => Option(new actions.SimpleCopy(actionConfig))
      case _ => None
    }
  }

}


