package com.cars.bigdata.turbocow.actions

import com.cars.bigdata.turbocow.Action
import com.cars.bigdata.turbocow.ActionContext
import com.cars.bigdata.turbocow.JsonUtil
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.JsonAST.JNothing
import org.json4s.jackson.JsonMethods._
import com.cars.bigdata.turbocow.ActionFactory
import com.cars.bigdata.turbocow.PerformResult
import com.cars.bigdata.turbocow._

import Lookup._

import scala.io.Source

class LookupMulti(
  val select: List[String],
  val fromDBTable: String,
  val fromFile: Option[String],
  val where: List[String],
  val equals: List[String],
  val onPass: ActionList = new ActionList,
  val onFail: ActionList = new ActionList
) extends Action {

  override def toString() = {
    
    val sb = new StringBuffer
    sb.append(s"""Lookup:{""")
    sb.append(s""", select(${select.mkString(", ")})""")
    select.foreach{ f => sb.append(f + ",") }
    sb.append(s""", fromDBTable(${fromDBTable})""")
    sb.append(s""", fromFile(${fromFile.getOrElse("<NONE>")})""")
    sb.append(s""", where(${where.mkString(", ")})""")
    sb.append(s""", equals(${equals.mkString(", ")})""")
    sb.append(s""", onPass = ${onPass.toString}""")
    sb.append(s""", onFail = ${onFail.toString}""")
    sb.append("}")
    sb.toString
  }

  // Check where & equals
  if (where.isEmpty) throw new Exception("lookup-multi has an empty 'where' list: "+this.toString)
  if (equals.isEmpty) throw new Exception("lookup-multi has an empty 'equals' list: "+this.toString)
  if (where.size != equals.size) throw new Exception("lookup-multi cannot have a 'where' list that is a different size than the 'equals' list: "+this.toString)

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

  //// The select fields separated by commas:
  //val fields = if(select.length > 1) {
  //  val table = "`" + tableName + "."
  //  table + select.mkString("`," + table) + "`"
  //}
  //else if(select.length == 1){
  //  select.head
  //}
  //else ""

  //// get all the fields needed in this table (select + where), without dups
  //val allFields = { 
  //  if (where != null && where.nonEmpty) {
  //    select :+ where
  //  }
  //  else select
  //}.distinct

  /** Get the lookup requirements
    */
  override def getLookupRequirements: List[CachedLookupRequirement] = {
    List(
      CachedLookupRequirement(
        fromDBTable, 
        Nil,
        select,
        fromFile,
        Set(where.toSet)
      )
    ) ++ onPass.getLookupRequirements ++ onFail.getLookupRequirements
  }
  
  /** Perform the lookup
    *
    */
  def perform(
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    PerformResult = { 

    PerformResult() }

  //  implicit val jsonFormats = org.json4s.DefaultFormats
  //
  //  // Get the values of source fields from the input JSON.
  //  // Sets it to null if not found.
  //  val lookupValueOpt = Option(equals.map{ e => 
  //    val nullStr: String = null
  //    JsonUtil.extractOption[String](inputRecord \ e).getOrElse(nullStr)
  //  })
  //
  //  // Get the table caches
  //  val caches = context.tableCaches
  //  if (caches.isEmpty) return PerformResult()
  //
  //  // get the table cache and do lookup
  //  val tableCacheOpt = caches.get(fromDBTable)
  //  tableCacheOpt.getOrElse{ throw new Exception("LookupMulti.perform:  couldn't find cached lookup table for: "+fromDBTable) } // TODO how to avoid exception??
  //  val tc = tableCacheOpt.get
  //
  //  // Get the selected fields out of the table
  //  val selectedFields: Option[Map[String, Option[String]]] = 
  //    tc.lookup(where, lookupValueOpt, select)
  //
  //  if (selectedFields.isEmpty || selectedFields.get.isEmpty) { // failed to find table or record
  //
  //    // Set the failure reason in the scratchpad for pickup later and 
  //    // possible rejection.
  //    val rejectReason = s"""Invalid $where: '${lookupValueOpt.getOrElse("")}'"""
  //    context.scratchPad.setResult("lookup", rejectReason)
  //
  //    onFail.perform(inputRecord, currentEnrichedMap, context)
  //  }
  //  else { // ok, found it
  //
  //    context.scratchPad.setResult("lookup", s"""Field '$where' exists in table '$fromDBTable':  '${lookupValueOpt.getOrElse("")}'""")
  //
  //    val enrichedAdditions = selectedFields.get.flatMap{ case(key, value) => 
  //      if (value.isEmpty) None
  //      else Some( (key, value.get) )
  //    }.toMap
  //
  //    onPass.perform(inputRecord, currentEnrichedMap ++ enrichedAdditions, context)
  //  }
  //}

}

object LookupMulti
{

  /** Alternate constructor to parse the Json config.
    */
  def apply(
    actionConfig: JValue, 
    actionFactory: Option[ActionFactory]): 
    LookupMulti = {

    val lookup = new LookupMulti(
      select = 
        (actionConfig \ "select").children.map{e => JsonUtil.extractString(e) },
      fromDBTable = JsonUtil.extractValidString(actionConfig \ "fromDBTable").getOrElse(throw new Exception("'lookup' action has empty 'fromDBTable' config item.")),
      fromFile = JsonUtil.extractOption[String](actionConfig \ "fromFile"),
      where = 
        (actionConfig \ "where").children.map{e => JsonUtil.extractString(e) },
      equals = 
        (actionConfig \ "equals").children.map{e => JsonUtil.extractString(e) },
      onPass = new ActionList(actionConfig \ "onPass", actionFactory),
      onFail = new ActionList(actionConfig \ "onFail", actionFactory)
    )
    lookup
  }

}


