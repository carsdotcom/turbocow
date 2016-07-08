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
import com.cars.turbocow._

import Lookup._

import scala.io.Source

class Lookup(
  val select: List[String],
  val fromDBTable: String,
  val fromFile: Option[String],
  val where: String,
  val equals: String,
  val onPass: ActionList = new ActionList,
  val onFail: ActionList = new ActionList
) extends Action {

  override def toString() = {
    
    val sb = new StringBuffer
    sb.append(s"""Lookup:{""")
    sb.append(s""", select = """)
    select.foreach{ f => sb.append(f + ",") }
    sb.append(s""", fromDBTable(${fromDBTable})""")
    sb.append(s""", fromFile(${fromFile.getOrElse("<NONE>")})""")
    sb.append(s""", where($where)""")
    sb.append(s""", onPass = ${onPass.toString}""")
    sb.append(s""", onFail = ${onFail.toString}""")
    sb.append("}")
    sb.toString
  }

  // Extract separate database and table names from fromDBTable:
  private val split = fromDBTable.split('.')
  val dbName: Option[String] = ValidString(
    if (split.size > 1) 
      Option(split.head) 
    else 
      None
  )
  val tableName = ValidString(
    if (split.size > 1) 
      Option(split.tail.mkString(".")) 
    else if (split.size==1) 
      Option(split(0)) 
    else 
      None
  ).getOrElse("Problem with 'lookup': couldn't determine table name from 'fromDBTable'.")

  // The select fields separated by commas:
  val fields = if(select.length > 1) {
    val table = "`" + tableName + "."
    table + select.mkString("`," + table) + "`"
  }
  else if(select.length == 1){
    select.head
  }
  else ""

  // get all the fields needed in this table (select + where), without dups
  val allFields = { 
    if (where != null && where.nonEmpty) {
      select :+ where
    }
    else select
  }.distinct

  /** Get the lookup requirements
    */
  override def getLookupRequirements: Option[CachedLookupRequirement] = {
    Option(
      CachedLookupRequirement(
        fromDBTable, 
        List(where),
        select,
        fromFile
      )
    ) 
  }

  /** Perform the lookup
    *
    */
  def perform(
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    PerformResult = {

    implicit val jsonFormats = org.json4s.DefaultFormats

    // get value of source field from the input JSON:
    val lookupValueOpt = JsonUtil.extractOption[String](inputRecord \ equals)
    // TODOTODO if getting this value fails.....  - lookup will fail.....

    // Get the table caches
    val caches = context.tableCaches
    if (caches.isEmpty) return PerformResult()

    // get the table cache and do lookup
    val tableCacheOpt = caches.get(fromDBTable)
    tableCacheOpt.getOrElse{ throw new Exception("Lookup.perform:  couldn't find cached lookup table for: "+fromDBTable) } // TODOTODO how to avoid exception??
    val tc = tableCacheOpt.get

    // Get the selected fields out of the table
    val selectedFields: Option[Map[String, Option[String]]] = 
      tc.lookup(where, lookupValueOpt.getOrElse(null), select)

    if (selectedFields.isEmpty || selectedFields.get.isEmpty) { // failed to find table or record

      // Set the failure reason in the scratchpad for pickup later and 
      // possible rejection.
      val rejectReason = s"""Invalid $where: '${lookupValueOpt.getOrElse("")}'"""
      context.scratchPad.setResult("lookup", rejectReason)

      onFail.perform(inputRecord, currentEnrichedMap, context)
    }
    else { // ok, found it

      context.scratchPad.setResult("lookup", s"""Field '$where' exists in table '$fromDBTable':  '${lookupValueOpt.getOrElse("")}'""")

      val enrichedAdditions = selectedFields.get.flatMap{ case(key, value) => 
        if (value.isEmpty) None
        else Some( (key, value.get) )
      }.toMap

      onPass.perform(inputRecord, currentEnrichedMap ++ enrichedAdditions, context)
    }
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
      select = 
        (actionConfig \ "select").children.map{e => JsonUtil.extractString(e) },
      fromDBTable = JsonUtil.extractValidString(actionConfig \ "fromDBTable").getOrElse(throw new Exception("'lookup' action has empty 'fromDBTable' config item.")),
      fromFile = JsonUtil.extractOption[String](actionConfig \ "fromFile"),
      where = JsonUtil.extractString(actionConfig \ "where"),
      equals = JsonUtil.extractValidString(actionConfig \ "equals").getOrElse("equals cannot be blank in 'lookup' action."),
      onPass = new ActionList(actionConfig \ "onPass", actionFactory),
      onFail = new ActionList(actionConfig \ "onFail", actionFactory)
    )
  }

}

