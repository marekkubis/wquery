package org.wquery.lang.operations

import scala.collection.mutable.Map
import scalaz._
import Scalaz._
import org.wquery.model.DataSet
import org.wquery.{FoundReferenceToUnknownVariableWhileEvaluatingException, WQueryEvaluationException, WQueryStaticCheckException}
import org.wquery.path.{TupleVariable, StepVariable}
import org.wquery.query.SetVariable

class Bindings(parent: Option[Bindings], updatesParent: Boolean) {
  val setVariables = Map[String, DataSet]()
  val tupleVariables = Map[String, List[Any]]()
  val stepVariables = Map[String, Any]()

  def bindStepVariable(name: String, value: Any) {
    if (updatesParent && parent.some(_.lookupStepVariable(name).isDefined).none(false)) {
      parent.get.bindStepVariable(name, value)
    } else {
      stepVariables(name) = value
    }
  }

  def bindTupleVariable(name: String, value: List[Any]) {
    if (updatesParent && parent.some(_.lookupTupleVariable(name).isDefined).none(false)) {
      parent.get.bindTupleVariable(name, value)
    } else {
      tupleVariables(name) = value
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

  def lookupTupleVariable(name: String): Option[List[Any]] = tupleVariables.get(name).orElse(parent.flatMap(_.lookupTupleVariable(name)))

  def lookupStepVariable(name: String): Option[Any] = stepVariables.get(name).orElse(parent.flatMap(_.lookupStepVariable(name)))

  def demandSetVariable(name: String): DataSet = {
    lookupSetVariable(name)|(throw new FoundReferenceToUnknownVariableWhileEvaluatingException(SetVariable(name)))
  }

  def demandTupleVariable(name: String): List[Any] = {
    lookupTupleVariable(name)|(throw new FoundReferenceToUnknownVariableWhileEvaluatingException(TupleVariable(name)))
  }

  def demandStepVariable(name: String): Any = {
    lookupStepVariable(name)|(throw new FoundReferenceToUnknownVariableWhileEvaluatingException(StepVariable(name)))
  }

}

object Bindings {
  def apply() = new Bindings(none, false)

  def apply(parent: Bindings, updatesParent: Boolean) = new Bindings(some(parent), updatesParent)
}
