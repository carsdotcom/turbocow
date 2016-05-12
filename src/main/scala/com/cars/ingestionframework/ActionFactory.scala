package com.cars.ingestionframework

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.io.Source

/** ActionFactory - creates all of the SourceActions based on the config file.
  *
  */
class ActionFactory {

  /** Create the list of SourceAction objects based on the config file.
    */
  def create(configFilePath: String): List[SourceAction] = {

    implicit val jsonFormats = org.json4s.DefaultFormats

    // parse it
    val configJson = Source.fromFile(configFilePath).getLines.mkString
    val configAST = parse(configJson)

    val itemsList = (configAST \ "items").children

    // transform to a list of SourceActions:
    itemsList.map{ item => 

      val sourceList = (item \ "source").children.map( _.values.toString)

      val actions = (item \ "actions").children.map{ jobj => 

        val actionType = (jobj \ "actionType").extract[String]
        val actionConfig = (jobj \ "config" )

        createActionForType(actionType, actionConfig)
      }

      SourceAction( sourceList, actions )
    }
  }

  /** Create an Action object based on the actionType and config from the json.
    * Note: config will be JNothing if not present.
    * Called from create(), above.
    */
  def createActionForType(actionType: String, actionConfig: JValue): Action = {

    // regexes:
    val replaceNullWithRE = """replace-null-with-([0-9]+)""".r

    actionType match {
      case "simple-copy" => new actions.SimpleCopy
      case replaceNullWithRE(number) => new actions.ReplaceNullWith(number.toInt)
      case _ => throw new RuntimeException("todo - what to do if specified action not found in code?  actionType = "+actionType)
    }
  }

}

