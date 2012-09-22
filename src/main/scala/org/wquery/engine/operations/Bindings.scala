package org.wquery.engine.operations

import scala.collection.mutable.Map
import scalaz._
import Scalaz._
import org.wquery.model.DataSet

class Bindings(parent: Option[Bindings], updatesParent: Boolean) {
  val setVariables = Map[String, DataSet]()
  val pathVariables = Map[String, List[Any]]()
  val stepVariables = Map[String, Any]()

  def bindStepVariable(name: String, value: Any) {
    if (updatesParent && parent.some(_.lookupStepVariable(name).isDefined).none(false)) {
      parent.get.bindStepVariable(name, value)
    } else {
      stepVariables(name) = value
    }
  }

  def bindPathVariable(name: String, value: List[Any]) {
    if (updatesParent && parent.some(_.lookupPathVariable(name).isDefined).none(false)) {
      parent.get.bindPathVariable(name, value)
    } else {
      pathVariables(name) = value
    }
  }

  def bindSetVariable(name: String, value: DataSet) {
    if (updatesParent && parent.some(_.lookupSetVariable(name).isDefined).none(false)) {
      parent.get.bindSetVariable(name, value)
    } else {
      setVariables(name) = value
    }
  }

  def lookupSetVariable(name: String): Option[DataSet] = setVariables.get(name).orElse(parent.flatMap(_.lookupSetVariable(name)))

  def lookupPathVariable(name: String): Option[List[Any]] = pathVariables.get(name).orElse(parent.flatMap(_.lookupPathVariable(name)))

  def lookupStepVariable(name: String): Option[Any] = stepVariables.get(name).orElse(parent.flatMap(_.lookupStepVariable(name)))
}

object Bindings {
  def apply() = new Bindings(none, false)

  def apply(parent: Bindings, updatesParent: Boolean) = new Bindings(some(parent), updatesParent)
}