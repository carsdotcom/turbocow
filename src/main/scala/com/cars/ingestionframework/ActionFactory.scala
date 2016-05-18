package com.cars.ingestionframework

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
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
  def create(configFilePath: String, hiveContext: HiveContext, sc : SparkContext): List[SourceAction] = {

    implicit val jsonFormats = org.json4s.DefaultFormats

    // parse it
    val configJson = sc.textFile(configFilePath).collect().mkString
    val configAST = parse(configJson)

    val itemsList = (configAST \ "items").children

    // transform to a list of SourceActions:
    itemsList.map{ item => 

      val sourceList = (item \ "source").children.map( _.values.toString)

      val actions = (item \ "actions").children.map{ jobj => 

        val actionType = (jobj \ "actionType").extract[String]
        val actionConfig = (jobj \ "config" )

        createActionForType(actionType, actionConfig, hiveContext)
      }

      SourceAction( sourceList, actions )
    }
  }

  /** Create an Action object based on the actionType and config from the json.
    * Note: config will be JNothing if not present.
    * Called from create(), above.
    */
  def createActionForType(actionType: String, actionConfig: JValue , hiveContext: HiveContext): Action = {

    // regexes:
    val replaceNullWithRE = """replace-null-with-([0-9]+)""".r

    actionType match {
      case "simple-copy" => new actions.SimpleCopy
      case "lookup" => new actions.Lookup(actionConfig, hiveContext)
      case replaceNullWithRE(number) => new actions.ReplaceNullWith(number.toInt)
      case "condition" => new actions.Condition(actionConfig)
      case _ => throw new RuntimeException("todo - what to do if specified action not found in code?  actionType = "+actionType)
    }
  }

}

