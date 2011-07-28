package org.wquery.engine

import scala.collection.mutable.Map

class Bindings(parent: Option[Bindings], updatesParent: Boolean) {
  val pathVariables = Map[String, List[Any]]()
  val stepVariables = Map[String, Any]()  
  private var contextVars = List[Any]()

  def bindStepVariable(name: String, value: Any) {
    if (updatesParent && parent.map(_.lookupStepVariable(name).isDefined).getOrElse(false)) {
      parent.get.bindStepVariable(name, value)
    } else {
      stepVariables(name) = value
    }
  }

  def bindPathVariable(name: String, value: List[Any]) {
    if (updatesParent && parent.map(_.lookupPathVariable(name).isDefined).getOrElse(false)) {
      parent.get.bindPathVariable(name, value)
    } else {
      pathVariables(name) = value
    }
  }

  def bindContextVariables(vars: List[Any]) = contextVars = vars
    
  def lookupPathVariable(name: String): Option[List[Any]] = pathVariables.get(name).orElse(parent.flatMap(_.lookupPathVariable(name)))
   
  def lookupStepVariable(name: String): Option[Any] = stepVariables.get(name).orElse(parent.flatMap(_.lookupStepVariable(name)))

  def lookupContextVariable(pos: Int): Option[Any] = if (contextVars.size - 1 - pos >= 0) Some(contextVars(contextVars.size - 1 - pos)) else parent.flatMap(_.lookupContextVariable(pos))
}

object Bindings {
  def apply() = new Bindings(None, false)
  
  def apply(parent: Bindings, updatesParent: Boolean) = new Bindings(Some(parent), updatesParent)
}