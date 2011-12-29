package org.wquery.engine

import scala.collection.mutable.Map
import scalaz._
import Scalaz._

class Bindings(parent: Option[Bindings], updatesParent: Boolean) {
  val pathVariables = Map[String, List[Any]]()
  val stepVariables = Map[String, Any]()  
  private var contextVariable: Option[Any] = none

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

  def bindContextVariable(variable: Any) = contextVariable = Some(variable)
    
  def lookupPathVariable(name: String): Option[List[Any]] = pathVariables.get(name).orElse(parent.flatMap(_.lookupPathVariable(name)))
   
  def lookupStepVariable(name: String): Option[Any] = stepVariables.get(name).orElse(parent.flatMap(_.lookupStepVariable(name)))

  def lookupContextVariable: Option[Any] = contextVariable.orElse(parent.flatMap(_.lookupContextVariable))
}

object Bindings {
  def apply() = new Bindings(None, false)
  
  def apply(parent: Bindings, updatesParent: Boolean) = new Bindings(Some(parent), updatesParent)
}