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
import com.cars.bigdata.turbocow.JsonUtil._

import scala.io.Source
import JdbcLookup._

import scala.util.Try

import com.cars.bigdata.turbocow.utils._

class JdbcLookup(
  val jdbcClient: String,
  val select: List[String],
  val fromDBTable: String,
  val where: String,
  val onPass: ActionList = new ActionList,
  val onFail: ActionList = new ActionList
) extends Action {

  if (onPass.actions.isEmpty && onFail.actions.isEmpty) throw new Exception("'jdbc-lookup': Must have at least one action in either onPass or onFail")

  // check the where to make sure the config is valid
  checkWhere(where)

  /** Json constructor
    */
  def this(
    actionConfig: JValue, 
    actionFactory: Option[ActionFactory]) = {

    this(
      jdbcClient = extractValidString(actionConfig \ "jdbcClient").getOrElse(throw new Exception("'jdbc-lookup': must provide a jdbcClient.")),
      select = {
        val jval = (actionConfig \ "select")
        jval match {
          case JArray(a) => ; // ok
          case JNothing => throw new Exception("'jdbc-lookup' action requires a 'select' config item")
          case _ => throw new Exception("'jdbc-lookup' action requires a 'select' config item that is an array of strings")
        }
        if (jval.children.isEmpty) throw new Exception("'jdbc-lookup' action requires a nonempty 'select' array")
        jval.children.map{e => extractString(e) }
      },
      fromDBTable = extractValidString(actionConfig \ "fromDBTable").getOrElse(throw new Exception("'jdbc-lookup' action has empty 'fromDBTable' config item.")),
      where = extractValidString(actionConfig \ "where").getOrElse(throw new Exception("'jdbc-lookup' action has empty 'where' config item")),
      onPass = new ActionList(actionConfig \ "onPass", actionFactory),
      onFail = new ActionList(actionConfig \ "onFail", actionFactory)
    )
  }

  /** toString
    */
  override def toString() = {
    
    val sb = new StringBuffer
    sb.append(s"""Lookup:{""")
    sb.append(s""", select = """)
    select.foreach{ f => sb.append(f + ",") }
    sb.append(s""", fromDBTable(${fromDBTable})""")
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
  ).getOrElse("Problem with 'jdbc-lookup': couldn't determine table name from 'fromDBTable'.")

  // The select fields separated by commas:
  val fields = if(select.length > 1) {
    val table = "`" + tableName + "."
    table + select.mkString("`," + table) + "`"
  }
  else if(select.length == 1){
    select.head
  }
  else ""

  //// get all the fields needed in this table (select + where), without dups
  //val allFields = { 
  //  if (where != null && where.nonEmpty) {
  //    select :+ where
  //  }
  //  else select
  //}.distinct

  /** Get the lookup requirements.  No caching is needed here but check the subactions.
    */
  override def getLookupRequirements: List[CachedLookupRequirement] = {
    onPass.getLookupRequirements ++ onFail.getLookupRequirements
  }

  def perform(
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    PerformResult = PerformResult()

//
//  /** Perform the lookup
//    *
//    */
//  def perform(
//    inputRecord: JValue, 
//    currentEnrichedMap: Map[String, String],
//    context: ActionContext): 
//    PerformResult = {
//
//    implicit val jsonFormats = org.json4s.DefaultFormats
//
//    // get value of source field from the input JSON:
//    val lookupValueOpt = extractOption[String](inputRecord \ equals)
//    // TODOTODO if getting this value fails.....  - lookup will fail.....
//
//    // Get the table caches
//    val caches = context.tableCaches
//    if (caches.isEmpty) return PerformResult()
//
//    // get the table cache and do lookup
//    val tableCacheOpt = caches.get(fromDBTable)
//    tableCacheOpt.getOrElse{ throw new Exception("Lookup.perform:  couldn't find cached lookup table for: "+fromDBTable) } // TODOTODO how to avoid exception??
//    val tc = tableCacheOpt.get
//
//    // Get the selected fields out of the table
//    val selectedFields: Option[Map[String, Option[String]]] = 
//      tc.lookup(where, lookupValueOpt, select)
//
//    if (selectedFields.isEmpty || selectedFields.get.isEmpty) { // failed to find table or record
//
//      // Set the failure reason in the scratchpad for pickup later and 
//      // possible rejection.
//      val rejectReason = s"""Invalid $where: '${lookupValueOpt.getOrElse("")}'"""
//      context.scratchPad.setResult("lookup", rejectReason)
//
//      onFail.perform(inputRecord, currentEnrichedMap, context)
//    }
//    else { // ok, found it
//
//      context.scratchPad.setResult("lookup", s"""Field '$where' exists in table '$fromDBTable':  '${lookupValueOpt.getOrElse("")}'""")
//
//      val enrichedAdditions = selectedFields.get.flatMap{ case(key, value) => 
//        if (value.isEmpty) None
//        else Some( (key, value.get) )
//      }.toMap
//
//      onPass.perform(inputRecord, currentEnrichedMap ++ enrichedAdditions, context)
//    }
//  }

}

object JdbcLookup {

  /** Check the text in the where clause to make sure it is valid.
    * Throw if not.
    * This should only be called from a constructor because it throws.
    */
  def checkWhere(where: String) = {
    // split on the '. 
    val quoteSplit = where.split("'")  

    // Every other element should be the thing in quotes.  Check it.
    var index = 1
    val error = "Unable to parse where-clause"
    (1 until quoteSplit.size by 2).foreach{ index =>
      val element = quoteSplit(index)

      // create a fieldsource.  If it doesn't throw, we're good.
      FieldSource.parseString(element, Option(FieldLocation.Constant))
    }
  }
}
