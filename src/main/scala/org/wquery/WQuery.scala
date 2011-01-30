package org.wquery
import org.wquery.engine.{WQueryFunctions, FunctionSet, DataSet, Result, Error, Answer, Context, Bindings}
import org.wquery.loader.{GridLoader, WordNetLoader}
import org.wquery.model.{WordNet, FunctionArgumentType}
import org.wquery.parser.WQueryParsers
import org.wquery.utils.Logging
import scala.collection.mutable.LinkedHashSet

class WQuery(wnet:WordNet) extends Logging {
  private val wordNet = wnet
  private val parser = new Object with WQueryParsers  
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
  
  def registerScalarFunction(name: String, args: List[FunctionArgumentType], result: FunctionArgumentType, clazz: java.lang.Class[_] , methodname: String) {  
    functions.addScalarFunction(name, args, result, clazz, methodname)
  }

  def registerAggregateFunction(name: String, args: List[FunctionArgumentType], result: FunctionArgumentType, clazz: java.lang.Class[_] , methodname: String) {  
    functions.addAggregateFunction(name, args, result, clazz, methodname)
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
