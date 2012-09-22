package org.wquery.engine.operations

import scala.collection.mutable.Map
import org.wquery.model.DataType
import org.wquery.WQueryStepVariableCannotBeBoundException
import org.wquery.engine._
import scala.Some

class BindingsPattern {
  val stepVariablesTypes = Map[String, Set[DataType]]()
  val pathVariablesTypes = Map[String, (AlgebraOp, Int, Int)]()
  val setVariablesTypes = Map[String, AlgebraOp]()

  def variables = {
    val stepVariables = stepVariablesTypes.keySet.map(StepVariable(_)).asInstanceOf[Set[Variable]]
    val pathVariables = pathVariablesTypes.keySet.map(TupleVariable(_)).asInstanceOf[Set[Variable]]

    stepVariables union pathVariables
  }

  def bindStepVariableType(name: String, types: Set[DataType]) {
    stepVariablesTypes(name) = types
  }

  def bindPathVariableType(name: String, op: AlgebraOp, leftShift: Int, rightShift: Int) {
    pathVariablesTypes(name) = (op, leftShift, rightShift)
  }

  def bindSetVariableType(name: String, op: AlgebraOp) {
    setVariablesTypes(name) = op
  }

  def lookupStepVariableType(name: String): Option[Set[DataType]] = stepVariablesTypes.get(name)

  def lookupPathVariableType(name: String): Option[(AlgebraOp, Int, Int)] = pathVariablesTypes.get(name)

  def lookupSetVariableType(name: String): Option[AlgebraOp] = setVariablesTypes.get(name)

  def isBound(variable: Variable) = variable match {
    case SetVariable(name) =>
      lookupSetVariableType(name).isDefined
    case TupleVariable(name) =>
      lookupPathVariableType(name).isDefined
    case StepVariable(name) =>
      lookupStepVariableType(name).isDefined
  }

  def union(that: BindingsPattern) = {
    val sum = BindingsPattern()

    sum.stepVariablesTypes ++= stepVariablesTypes
    sum.stepVariablesTypes ++= that.stepVariablesTypes
    sum.pathVariablesTypes ++= pathVariablesTypes
    sum.pathVariablesTypes ++= that.pathVariablesTypes
    sum.setVariablesTypes ++= setVariablesTypes
    sum.setVariablesTypes ++= that.setVariablesTypes

    sum
  }

  def bindVariablesTypes(variables: VariableTemplate, op: AlgebraOp) {
    variables.pathVariablePosition match {
      case Some(pathVarPos) =>
        bindTypesFromRight(op, variables)
        variables.pathVariableName.map(bindPathVariableType(_, op, variables.leftPatternSize, variables.rightPatternSize))
        bindTypesFromLeft(op, variables)
      case None =>
        bindTypesFromRight(op, variables)
    }
  }

  private def bindTypesFromLeft(op: AlgebraOp, variables: VariableTemplate) {
    for ((name, pos) <- variables.leftVariablesIndexes) {
      if (op.maxTupleSize.map(pos < _).getOrElse(true))
        bindStepVariableType(name, op.leftType(pos))
      else
        throw new WQueryStepVariableCannotBeBoundException(name)
    }
  }

  private def bindTypesFromRight(op: AlgebraOp, variables: VariableTemplate) {
    for ((name, pos) <- variables.rightVariablesIndexes) {
      if (op.maxTupleSize.map(pos < _).getOrElse(true))
        bindStepVariableType(name, op.rightType(pos))
      else
        throw new WQueryStepVariableCannotBeBoundException(name)
    }
  }
}

object BindingsPattern {
  def apply() = new BindingsPattern
}