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

  // My private exception class:
  class LookupFailure(msg: String) extends RuntimeException(msg)

  if (onPass.actions.isEmpty && onFail.actions.isEmpty) throw new Exception(s"'$actionName': Must have at least one action in either onPass or onFail")

  // check the where to make sure the config is valid
  checkWhere(where)

  /** Json constructor
    */
  def this(
    actionConfig: JValue, 
    actionFactory: Option[ActionFactory]) = {

    this(
      jdbcClient = extractValidString(actionConfig \ "jdbcClient").getOrElse(throw new Exception(s"'$actionName': must provide a jdbcClient.")),
      select = {
        val jval = (actionConfig \ "select")
        jval match {
          case JArray(a) => ; // ok
          case JNothing => throw new Exception(s"'$actionName' action requires a 'select' config item")
          case _ => throw new Exception(s"'$actionName' action requires a 'select' config item that is an array of strings")
        }
        if (jval.children.isEmpty) throw new Exception(s"'$actionName' action requires a nonempty 'select' array")
        jval.children.map{e => extractString(e) }
      },
      fromDBTable = extractValidString(actionConfig \ "fromDBTable").getOrElse(throw new Exception(s"'$actionName' action has empty 'fromDBTable' config item.")),
      where = extractValidString(actionConfig \ "where").getOrElse(throw new Exception(s"'$actionName' action has empty 'where' config item")),
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

  //// Extract separate database and table names from fromDBTable:
  //private val split = fromDBTable.split('.')
  //val dbName: Option[String] = ValidString(
  //  if (split.size > 1) 
  //    Option(split.head) 
  //  else 
  //    None
  //)
  //val tableName = ValidString(
  //  if (split.size > 1) 
  //    Option(split.tail.mkString(".")) 
  //  else if (split.size==1) 
  //    Option(split(0)) 
  //  else 
  //    None
  //).getOrElse(s"Problem with '$actionName': couldn't determine table name from 'fromDBTable'.")
  //
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

  /** Get the lookup requirements.  No caching is needed here but check the subactions.
    */
  override def getLookupRequirements: List[CachedLookupRequirement] = {
    onPass.getLookupRequirements ++ onFail.getLookupRequirements
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

    def handleFailure(e: Throwable): PerformResult = {
      context.scratchPad.setResult(actionName, e.getMessage)
      onFail.perform(inputRecord, currentEnrichedMap, context)
    }

    try {
      // get the jdbc client (Statement instance)
      val statement = context.jdbcClients.get(jdbcClient).getOrElse(throw new LookupFailure(s"couldn't do JDBC lookup:  database client not found by name of '$jdbcClient'"))

      val query = createQuery(inputRecord, currentEnrichedMap, context).get

      // perform the query
      val resultSet = statement.executeQuery(query)

      // get all the fields from select list as a list of (fieldname, option-strings).
      // I know there's vars... jdbc api forces it.  (Need to count the number of rows)
      var recordCount = 0
      var resultsList = List.empty[(String, Option[String])]
      while ( resultSet.next() ) {
        recordCount += 1
        if (recordCount == 1) {
          resultsList = (1 to select.size).toList.map{ index => 
            (select(index-1), Option(resultSet.getString(index)))
          }
        } 
      }

      // Check the record count
      if (recordCount == 0) throw new LookupFailure(s"$actionName failed: no records found for query: "+query)
      if (recordCount >= 2) throw new LookupFailure(s"$actionName failed: too many records found ($recordCount; expected 1) for query: $query")

      // check if any fields were not found (resultSet.getString returned null, gave None)
      val listOfNoneFields = resultsList.filter( _._2 == None).map( _._1 )
      if (listOfNoneFields.nonEmpty) throw new LookupFailure(s"$actionName failed: fields ("+listOfNoneFields.mkString(", ")+") returned null as a result of query: "+query)

      // Success; set result and run onPass...
      context.scratchPad.setResult(actionName, s"""$actionName query succeeded:  $query""")
      val enrichedAdditions = resultsList.map{ e=> (e._1, e._2.get) }.toMap
    
      onPass.perform(inputRecord, currentEnrichedMap ++ enrichedAdditions, context)
    } 
    catch {
      case e: LookupFailure => {
        println("LookupFailure: "+e.getMessage)
        handleFailure(e)
      }
      case e: Throwable => {
        println("General Jdbc Lookup Exception: "+e.getMessage)
        handleFailure(e)
      }
    }
  }

  /** create a query string based on this object and perform params
    * @return Success if the query was able to be created successfully; 
    *         Failure if any of the input values for the where-clause could not 
    *         be read, and therefore a valid query couldn't be constructed.
    */
  def createQuery(
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): 
    Try[String] = {

    // create the query
    val querySB = new StringBuilder()
    querySB.append("select ")
    querySB.append(select.mkString(","))
    querySB.append(s" from $fromDBTable")
    querySB.append(s" where ")

    // fill in the where values inside the quotes with actual strings
    val quoteSplit = where.split("'")
    Try {
      (0 until quoteSplit.size).foreach{ index =>
        val element = quoteSplit(index)
        index % 2 match {
          case 0 => { // even
            querySB.append(element)
          }
          case 1 => { // odd
            val fs = FieldSource.parseString(element, Option(JdbcLookup.defaultLocation))
            val value = fs.getValue(inputRecord, currentEnrichedMap, context.scratchPad).getOrElse(throw new Exception(s"couldn't find input value for where-value in $actionName: '"+element+"'"))
            querySB.append(s"'$value'")
          }
        }
      }

      val query = querySB.toString
      println(s"query is [$query]")
      query
    }
  }
}

object JdbcLookup {

  val defaultLocation = FieldLocation.Constant
  val actionName = "jdbc-lookup"

  /** Check the text in the where clause to make sure it is valid.
    * Throw if not.
    * This should only be called from a constructor because it throws.
    */
  def checkWhere(where: String) = {
    // split on the '. 
    val quoteSplit = where.split("'")  

    // Every other element should be the thing in quotes.  Check it.
    val error = "Unable to parse where-clause"
    (1 until quoteSplit.size by 2).foreach{ index =>
      val element = quoteSplit(index)

      // create a fieldsource.  If it doesn't throw, we're good.
      FieldSource.parseString(element, Option(JdbcLookup.defaultLocation))
    }
  }

  /** Get the value from a where string, ie. the Y in "where X = 'Y'".
    * Normally called from perform(), above
    */
  def getWhereValue(
    whereStr: String,
    inputRecord: JValue, 
    currentEnrichedMap: Map[String, String],
    context: ActionContext): Option[String] = {

    val fs = FieldSource.parseString(whereStr, Option(JdbcLookup.defaultLocation))
    fs.getValue(inputRecord, currentEnrichedMap, context.scratchPad)
  }
}

