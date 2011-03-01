package org.wquery
import org.wquery.engine.{WQueryFunctions, Result, Error, Answer, Bindings}
import org.wquery.loader.{GridLoader, WordNetLoader}
import org.wquery.model.{WordNet, FunctionArgumentType, DataSet}
import org.wquery.parser.WQueryParsers
import org.wquery.utils.Logging
import scala.collection.mutable.LinkedHashSet

class WQuery(val wordNet: WordNet) extends Logging {
  private val parser = new Object with WQueryParsers  
  private val bindings = Bindings()
  
  def execute(input:String): Result = {    
    try {
      val expr = parser parse input    
      debug(expr.toString)           
      Answer(expr.evaluate(wordNet, bindings))
    } catch {
      case e: WQueryException => Error(e)
    }
  } 
  
  def registerScalarFunction(name: String, args: List[FunctionArgumentType], result: FunctionArgumentType, clazz: java.lang.Class[_] , methodname: String) {  
    bindings.bindScalarFunction(name, args, result, clazz, methodname)
  }

  def registerAggregateFunction(name: String, args: List[FunctionArgumentType], result: FunctionArgumentType, clazz: java.lang.Class[_] , methodname: String) {  
    bindings.bindAggregateFunction(name, args, result, clazz, methodname)
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
