package org.wquery.engine
import scala.collection.mutable.Map

class Bindings(parent: Option[Bindings]) {
  val pathVariables = Map[String, List[Any]]()
  val stepVariables = Map[String, Any]()  
  val relationalExprAliases = Map[String, RelationalExpr]()

  def bindPathVariable(name: String, value: List[Any]) = (pathVariables(name) = value)

  def bindStepVariable(name: String, value: Any) = (stepVariables(name) = value) 

  def bindRelationalExprAlias(name: String, value: RelationalExpr) = (relationalExprAliases(name) = value)  
  
  def lookupPathVariable(name: String): Option[List[Any]] = pathVariables.get(name).orElse(parent.flatMap(_.lookupPathVariable(name)))
   
  def lookupStepVariable(name: String): Option[Any] = stepVariables.get(name).orElse(parent.flatMap(_.lookupStepVariable(name)))
  
  def lookupRelationalExprAlias(name: String): Option[RelationalExpr] = relationalExprAliases.get(name).orElse(parent.flatMap(_.lookupRelationalExprAlias(name)))   
}

object Bindings {
  def apply() = new Bindings(None)
  
  def apply(parent: Bindings) = new Bindings(Some(parent))  
}