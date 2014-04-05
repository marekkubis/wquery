package org.wquery.lang.operations

import scala.collection.mutable.Map
import org.wquery.model.DataType
import org.wquery.WQueryStepVariableCannotBeBoundException
import org.wquery.lang.Variable
import org.wquery.path.StepVariable
import org.wquery.path.TupleVariable
import org.wquery.path.VariableTemplate
import org.wquery.query.SetVariable
import scala.Some
import scalaz._
import Scalaz._

class BindingsPattern {
  val stepVariablesTypes = Map[String, Set[DataType]]()
  val tupleVariablesTypes = Map[String, (AlgebraOp, Int, Int)]()
  val setVariablesTypes = Map[String, AlgebraOp]()

  def variables = {
    val stepVariables = stepVariablesTypes.keySet.map(StepVariable(_)).asInstanceOf[Set[Variable]]
    val pathVariables = tupleVariablesTypes.keySet.map(TupleVariable(_)).asInstanceOf[Set[Variable]]

    stepVariables union pathVariables
  }

  def bindStepVariableType(name: String, types: Set[DataType]) {
    stepVariablesTypes(name) = types
  }

  def bindTupleVariableType(name: String, op: AlgebraOp, leftShift: Int, rightShift: Int) {
    tupleVariablesTypes(name) = (op, leftShift, rightShift)
  }

  def bindSetVariableType(name: String, op: AlgebraOp) {
    setVariablesTypes(name) = op
  }

  def lookupStepVariableType(name: String): Option[Set[DataType]] = stepVariablesTypes.get(name)

  def lookupTupleVariableType(name: String): Option[(AlgebraOp, Int, Int)] = tupleVariablesTypes.get(name)

  def lookupSetVariableType(name: String): Option[AlgebraOp] = setVariablesTypes.get(name)

  def isBound(variable: Variable) = variable match {
    case SetVariable(name) =>
      lookupSetVariableType(name).isDefined
    case TupleVariable(name) =>
      lookupTupleVariableType(name).isDefined
    case StepVariable(name) =>
      lookupStepVariableType(name).isDefined
  }

  def union(that: BindingsPattern) = {
    val sum = BindingsPattern()

    sum.stepVariablesTypes ++= stepVariablesTypes
    sum.stepVariablesTypes ++= that.stepVariablesTypes
    sum.tupleVariablesTypes ++= tupleVariablesTypes
    sum.tupleVariablesTypes ++= that.tupleVariablesTypes
    sum.setVariablesTypes ++= setVariablesTypes
    sum.setVariablesTypes ++= that.setVariablesTypes

    sum
  }

  def bindVariablesTypes(variables: VariableTemplate, op: AlgebraOp) {
    variables.pathVariablePosition match {
      case Some(pathVarPos) =>
        bindTypesFromRight(op, variables)
        variables.pathVariableName.map(bindTupleVariableType(_, op, variables.leftPatternSize, variables.rightPatternSize))
        bindTypesFromLeft(op, variables)
      case None =>
        bindTypesFromRight(op, variables)
    }
  }

  private def bindTypesFromLeft(op: AlgebraOp, variables: VariableTemplate) {
    for ((name, pos) <- variables.leftVariablesIndexes) {
      if (op.maxTupleSize.some(pos < _).none(true))
        bindStepVariableType(name, op.leftType(pos))
      else
        throw new WQueryStepVariableCannotBeBoundException(name)
    }
  }

  private def bindTypesFromRight(op: AlgebraOp, variables: VariableTemplate) {
    for ((name, pos) <- variables.rightVariablesIndexes) {
      if (op.maxTupleSize.some(pos < _).none(true))
        bindStepVariableType(name, op.rightType(pos))
      else
        throw new WQueryStepVariableCannotBeBoundException(name)
    }
  }
}

object BindingsPattern {
  def apply() = new BindingsPattern
}
