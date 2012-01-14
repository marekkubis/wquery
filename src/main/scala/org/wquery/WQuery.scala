package org.wquery

import engine._
import loader.{LmfLoader, GridLoader, WordNetLoader}
import model.impl.InMemoryWordNetStore
import operations.{Bindings, BindingsSchema}
import org.wquery.model.WordNet
import org.wquery.parser.WQueryParsers
import org.wquery.utils.Logging
import scala.collection.mutable.LinkedHashSet

class WQuery(val wordNet: WordNet) extends Logging {
  val bindingsSchema = BindingsSchema()
  val bindings = Bindings()
  val parser = new Object with WQueryParsers
  
  def execute(input:String): Result = {    
    try {
      val expr = parser parse input
      debug("Expr: " + expr)

      val plan = expr.evaluationPlan(wordNet.schema, bindingsSchema, Context())
      debug("Plan: " + plan)

      val dataSet = plan.evaluate(wordNet, bindings)
      debug("Eval: " + dataSet.pathCount + " tuple(s) found")

      Answer(wordNet, dataSet)
    } catch {
      case e: WQueryException => Error(e)
    }
  } 
}

object WQuery {
  val loaders = LinkedHashSet[WordNetLoader]()

  registerLoader(new LmfLoader)
  registerLoader(new GridLoader)
  
  def registerLoader(loader: WordNetLoader) { loaders += loader }
  
  def unregisterLoader(loader: WordNetLoader) { loaders -= loader }    

  def getInstance(from: String): WQuery = {
    createInstance(from: String) // TODO temporary stub
  }

  def createInstance(from: String, where: String = "memory"): WQuery = {
    val store = where match {
      case _ =>
        new InMemoryWordNetStore
    }

    val wordNet = new WordNet(store)

    for (loader <- loaders) {
      if (loader.canLoad(from)) {
        loader.load(from, wordNet)
        return new WQuery(wordNet)
      }
    }
    
    throw new WQueryLoadingException(from + " cannot be loaded by any loader")
  }
}
