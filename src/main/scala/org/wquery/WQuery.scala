package org.wquery

import loader.{LmfLoader, GridLoader, WordNetLoader}
import org.wquery.engine.{WQueryFunctions, Result, Error, Answer, Bindings}
import org.wquery.model.WordNet
import org.wquery.parser.WQueryParsers
import org.wquery.utils.Logging
import scala.collection.mutable.LinkedHashSet

class WQuery(val wordNet: WordNet) extends Logging {
  val bindings = Bindings()
  
  WQueryFunctions.functions.foreach(f => bindings.bindFunction(f._1, f._2, f._3))
  
  private val parser = new Object with WQueryParsers  
  
  def execute(input:String): Result = {    
    try {
      val expr = parser parse input    
      debug("Expr: " + expr)

      val plan = expr.evaluationPlan(wordNet, bindings)
      debug("Plan: " + plan)

      Answer(plan.evaluate(wordNet, bindings))
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
  
  def getInstance(url: String): WQuery = {
    for (loader <- loaders) {
      if (loader.canLoad(url)) {
        return new WQuery(loader.load(url))
      }
    }
    
    throw new WQueryLoadingException(url + " cannot be loaded by any loader")
  }
}
