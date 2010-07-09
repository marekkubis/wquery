package org.wquery

import scala.collection.mutable.LinkedHashSet
import org.wquery.engine.Bindings
import org.wquery.engine.Context
import org.wquery.engine.Answer
import org.wquery.engine.Error
import org.wquery.engine.Result
import org.wquery.engine.DataSet
import org.wquery.engine.FunctionSet
import org.wquery.engine.WQueryFunctions
import org.wquery.loader.WordNetLoader
import org.wquery.loader.GridLoader
import org.wquery.model.FunctionArgumentType
import org.wquery.model.FunctionType
import org.wquery.model.WordNet
import org.wquery.parser.WQueryParsers
import org.wquery.utils.Logging

class WQuery(wnet:WordNet) extends Logging {
  private val wordNet = wnet
  private val parser = new WQueryParsers  
  private val bindings = new Bindings(None)
  private val functions = new FunctionSet
  
  def execute(input:String): Result = {    
    try {
      val expr = parser parse input    
      debug(expr.toString)           
      Answer(expr.evaluate(functions, wordNet, bindings, Context.empty))
    } catch {
      case e: WQueryException => Error(e)
    }
  } 
  
  def registerFunction(name: String, args: List[FunctionArgumentType], result: FunctionArgumentType, 
                       ftype: FunctionType, clazz: java.lang.Class[_] , methodname: String) {  
    functions.addFunction(name, args, result, ftype, clazz, methodname)
  }

  def unregisterFunction(name: String, args: List[FunctionArgumentType]) { 
    functions.removeFunction(name, args) 
  }
}

object WQuery {
  val loaders = LinkedHashSet[WordNetLoader]()
  
  registerLoader(new GridLoader)
  
  def registerLoader(loader: WordNetLoader) { loaders += loader }
  
  def unregisterLoader(loader: WordNetLoader) { loaders -= loader }    
  
  def getInstance(url: String): WQuery = {
    for (loader <- loaders) {
      if (loader.canLoad(url)) {
        val wquery = new WQuery(loader.load(url))
        WQueryFunctions.registerFunctionsIn(wquery)
        return wquery
      }
    }
    
    throw new WQueryLoadingException(url + " cannot be loaded by any loader")
  }
}
