package org.wquery.engine
import scala.collection.mutable.Map

class Bindings(parent: Option[Bindings]) {  
  val pathVariables = Map[String, List[Any]]()
  val stepVariables = Map[String, Any]()  
  val relationalExprAliases = Map[String, RelationalExpr]()
  
  private var contextVars = List[Any]()

  def bindPathVariable(name: String, value: List[Any]) = (pathVariables(name) = value)

  def bindStepVariable(name: String, value: Any) = (stepVariables(name) = value) 

  def bindRelationalExprAlias(name: String, value: RelationalExpr) = (relationalExprAliases(name) = value)
  
  def bindContextVariables(vars: List[Any]) = contextVars = vars
    
  def lookupPathVariable(name: String): Option[List[Any]] = pathVariables.get(name).orElse(parent.flatMap(_.lookupPathVariable(name)))
   
  def lookupStepVariable(name: String): Option[Any] = stepVariables.get(name).orElse(parent.flatMap(_.lookupStepVariable(name)))
  
  def lookupRelationalExprAlias(name: String): Option[RelationalExpr] = relationalExprAliases.get(name).orElse(parent.flatMap(_.lookupRelationalExprAlias(name)))
  
  def lookupContextVariable(pos: Int) = if (contextVars.size - pos >= 0) Some(contextVars(contextVars.size - pos)) else None
  
  def areContextVariablesBound = contextVars != Nil
  
  def contextVariables = contextVars
}

object Bindings {
  def apply() = new Bindings(None)
  
  def apply(parent: Bindings) = new Bindings(Some(parent))  
}