package com.cars.ingestionframework

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.io.Source

/** ActionFactory - creates all of the SourceActions based on the config file.
  * 
  * @param customActionCreators a list of custom ActionCreator objects to be 
  *        used, in order, before checking against the standard framework 
  *        actions.  Note that you can override the creation of standard framework 
  *        actions by indicating 
  */
class ActionFactory(customActionCreators: List[ActionCreator] = List.empty[ActionCreator]) 
  extends ActionCreator {

  /** Create the list of SourceAction objects based on the config file.
    */
  def createSourceActions(configFilePath: String): List[SourceAction] = {

    implicit val jsonFormats = org.json4s.DefaultFormats

    // parse it
    val configJson = Source.fromFile(configFilePath).getLines.mkString
    val configAST = parse(configJson)

    val itemsList = (configAST \ "items").children

    // transform to a list of SourceActions:
    itemsList.map{ item => 

      val sourceList = (item \ "source").children.map( _.values.toString)

      val actions = (item \ "actions").children.map{ jobj => 

        // Get the info for this action to send to the action creator.
        val actionType = (jobj \ "actionType").extract[String]
        val actionConfig = (jobj \ "config" )

        // First, attempt to create an action using custom creators, if any:
        val customAction: Option[Action] = if (customActionCreators.nonEmpty) {
          var action: Option[Action] = None
          var creatorIter = customActionCreators.iterator
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

      SourceAction( sourceList, actions )
    }
  }

  /** Create an Action object based on the actionType and config from the json.
    * Note: config will be JNothing if not present.
    * Called from create(), above.
    * All standard actions supported in the framework should be created here.
    */
  override def createAction(actionType: String, actionConfig: JValue): 
    Option[Action] = {
  
    // regexes:
    val replaceNullWithRE = """replace-null-with-([0-9]+)""".r
  
    actionType match {
      case "simple-copy" => Option(new actions.SimpleCopy)
      case "lookup" => Option(new actions.Lookup(actionConfig))
      case replaceNullWithRE(number) => Option(new actions.ReplaceNullWith(number.toInt))
      case _ => None
    }
  }
}

/** Companion object for additional constructors.
  * 
  */
object ActionFactory {

  /** Helper constructor to instantiate with just one custom creator
    */
  def apply(singleActionCreator: ActionCreator): ActionFactory =
    new ActionFactory(List(singleActionCreator))
}

