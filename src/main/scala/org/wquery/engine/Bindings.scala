package org.wquery.engine
import scala.collection.mutable.Map

class Bindings(parent: Option[Bindings]) {
  val pathVariables = Map[String, List[Any]]()
  val stepVariables = Map[String, Any]()  
  val relationalExprAliases = Map[String, RelationalExpr]()

  def bindPathVariable(name: String, value: List[Any]) = (pathVariables(name) = value)

  def bindStepVariable(name: String, value: Any) = (stepVariables(name) = value) 

  def bindRelationalExprAlias(name: String, value: RelationalExpr) = (relationalExprAliases(name) = value)  
  
  def lookupPathVariable(name: String): Option[List[Any]] = lookup[List[Any]](name, pathVariables, parent match {case Some(parent) => Some(parent.lookupPathVariable _) case None => None} ) 

  def lookupStepVariable(name: String): Option[Any] = lookup[Any](name, stepVariables, parent match {case Some(parent) => Some(parent.lookupStepVariable _) case None => None} )   
  
  def lookupRelationalExprAlias(name: String): Option[RelationalExpr] = lookup[RelationalExpr](name, relationalExprAliases, parent match {case Some(parent) => Some(parent.lookupRelationalExprAlias _) case None => None} )   
    
  private def lookup[A](name: String, map: Map[String, A], func: Option[String => Option[A]]): Option[A] = {
    if (map contains name) {
      Some(map(name))
    } else {
      func match {
        case Some(func) =>
          func(name)
        case None =>
          None
      }      
    }
  }  
}

object Bindings {
  def apply() = new Bindings(None)
  
  def apply(parent: Bindings) = new Bindings(Some(parent))  
}