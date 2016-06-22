package com.cars.turbocow.actions

import com.cars.turbocow.Action
import com.cars.turbocow.ActionContext
import com.cars.turbocow.JsonUtil
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.JsonAST.JNothing
import org.json4s.jackson.JsonMethods._
import com.cars.turbocow.ActionFactory
import com.cars.turbocow.PerformResult

import Lookup._

import scala.io.Source

class Lookup(
  val lookupFile: Option[String],
  val lookupDB: Option[String],
  val lookupTable: Option[String],
  val where: String,
  val equals: String,
  val select: List[String],
  val onPass: SubActionList = new SubActionList,
  val onFail: SubActionList = new SubActionList
) extends Action {

  override def toString() = {
    
    val sb = new StringBuffer
    sb.append(s"""Lookup:{lookupFile(${lookupFile.getOrElse("<NONE>")})""")
    sb.append(s""", lookupDB(${lookupDB.getOrElse("<NONE>")})""")
    sb.append(s""", lookupTable(${lookupTable.getOrElse("<NONE>")})""")
    sb.append(s""", where($where)""")
    sb.append(s""", select = """)
    select.foreach{ f => sb.append(f + ",") }
    sb.append(s""", onPass = ${onPass.toString}""")
    sb.append(s""", onFail = ${onFail.toString}""")
    sb.append("}")
    sb.toString
  }

  val fields = if(select.length > 1) {
    val tableName = lookupTable match {
      case None => ""
      case some => "`" + some.get + "."
    }
    tableName + select.mkString("`," + tableName)
  }
  else if(select.length == 1){
    select(0)
  }
  else ""

  // "db.table", or "lookupFile"... todo rename....
  val dbAndTable = 
    if (lookupFile.nonEmpty)
      lookupFile.get
    else if (lookupDB.nonEmpty && lookupTable.nonEmpty) 
      s"${lookupDB.get}.${lookupTable.get}"
    else
      throw new Exception(s"couldn't find lookupDB($lookupDB) or lookupTable($lookupTable)")

  // get all the fields needed in this table (select + where), without dups
  val allFields = { 
    if (where != null && where.nonEmpty) {
      select :+ where
    }
    else select
  }.distinct

  /** Perform the lookup
    *
    */
  def perform(
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    PerformResult = {

    implicit val jsonFormats = org.json4s.DefaultFormats

    val sourceFields = List(equals) // todo refactor the below to not use a list

    val enrichedUpdates = sourceFields.flatMap{ field => 

      // search in the table for this key
      lookupFile match {
        case None => { // do hdfs "lookup"
        
          val caches = context.tableCaches
          if (caches.isEmpty) {
            // return empty map - can't look up into nonexistent tables!  (todo - reject or throw?)
            Map.empty[String, String]
          }
          else {  // cache is not empty

            lookupDB.getOrElse{ throw new Exception("TODO - reject this because lookupDB not found in config") }
            lookupTable.getOrElse{ throw new Exception("TODO - reject this because lookupTable not found in config") }

            val lookupDBAndTable = dbAndTable
            val tableCacheOpt = caches.get(lookupDBAndTable)
            tableCacheOpt.getOrElse{ throw new Exception("couldn't find cached lookup table for: "+lookupDBAndTable) }
                                   
            // get the table cache and do lookup
            val tc = tableCacheOpt.get
            // TODOTODO this is forced to a fixed type... how to resolve?
            val lookupValue = JsonUtil.extractOption[String](inputRecord \ sourceFields.head)
            // TODOTODO if getting this value fails..... ?

            // todo what if not there, todo check the enriched record first
            select.map{ field => 
              val resultOpt = tc.lookup(
                where, 
                lookupValue.get.toString,
                field)
              if (resultOpt.isEmpty) Map.empty[String, String]
              else Map(field -> resultOpt.get)
            }.reduce( _ ++ _ ) // combine all maps into one
          }

          //TODO - change status accepted or rejected
        }
        case _ => { // local file lookup
        
          // look up local file and parse as json.
          val configAST = parse(Source.fromFile(lookupFile.get).getLines.mkString)

          // get value of source field from the input JSON:
          val lookupValue = JsonUtil.extractOption[String](inputRecord \ sourceFields.head)

          val dimRecord: Option[JValue] = 
            if( lookupValue.isEmpty ) None
            else configAST.children.find( record => (record \ where) == JString(lookupValue.get) )

          if (dimRecord.isEmpty) { // failed

            // Set the failure reason in the scratchpad for pickup later and 
            // possible rejection.
            val rejectReason = s"""Invalid $where: '${lookupValue.getOrElse("")}'"""
            context.scratchPad.setResult("lookup", rejectReason)

            if (onFail.actions.isEmpty) {
              // return an empty list of tuples (that will be converted to an empty map later)
              List.empty[Tuple2[String, String]]
            }
            else { // have onFail actions.  Run them all.
              // todo add this to SubActionList?
              onFail.actions.map{ action => 
                action.perform(inputRecord, currentEnrichedMap, context).enrichedUpdates.toList
              }.foldLeft( List.empty[Tuple2[String, String]] )( _ ++ _ )
            }
          }
          else { // ok, found it

            context.scratchPad.setResult("lookup", s"""Field '$where' exists in table '$dbAndTable':  '${lookupValue.getOrElse("")}'""")

            select.map{ selectField => 
              val fieldVal = (dimRecord.get \ selectField).extract[String]
              (selectField, fieldVal)
            }

            // todotodo run onPass actions
          }
        }
      }

    }.toMap

    PerformResult(enrichedUpdates)
  }

}

object Lookup
{

  /** Alternate constructor to parse the Json config.
    */
  def apply(
    actionConfig: JValue, 
    actionFactory: Option[ActionFactory]): 
    Lookup = {

    new Lookup(
      lookupFile = JsonUtil.extractOption[String](actionConfig \ "lookupFile"),
      lookupDB = JsonUtil.extractOption[String](actionConfig \ "lookupDB"),
      lookupTable = JsonUtil.extractOption[String](actionConfig \ "lookupTable"),
      where = JsonUtil.extractString(actionConfig \ "where"),
      equals = JsonUtil.extractValidString(actionConfig \ "equals").getOrElse("equals cannot be blank in 'lookup' action."),
      select = 
        (actionConfig \ "select").children.map{e => JsonUtil.extractString(e) },
      onPass = new SubActionList(actionConfig \ "onPass", actionFactory),
      onFail = new SubActionList(actionConfig \ "onFail", actionFactory)
    )
  }

}

