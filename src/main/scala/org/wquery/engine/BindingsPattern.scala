package org.wquery.engine

import scala.collection.mutable.Map
import org.wquery.model.DataType
import org.wquery.WQueryStaticCheckException

class BindingsPattern {
  val stepVariablesTypes = Map[String, Set[DataType]]()
  val pathVariablesTypes = Map[String, (AlgebraOp, Int, Int)]()

  def variables = {
    val stepVariables = stepVariablesTypes.keySet.map(StepVariable(_)).asInstanceOf[Set[Variable]]
    val pathVariables = pathVariablesTypes.keySet.map(PathVariable(_)).asInstanceOf[Set[Variable]]

    stepVariables union pathVariables
  }

  def bindStepVariableType(name: String, types: Set[DataType]) {
    stepVariablesTypes(name) = types
  }

  def bindPathVariableType(name: String, op: AlgebraOp, leftShift: Int, rightShift: Int) {
    pathVariablesTypes(name) = (op, leftShift, rightShift)
  }

  def lookupStepVariableType(name: String): Option[Set[DataType]] = stepVariablesTypes.get(name)

  def lookupPathVariableType(name: String): Option[(AlgebraOp, Int, Int)] = pathVariablesTypes.get(name)

  def isBound(variable: Variable) = variable match {
    case PathVariable(name) =>
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

    sum
  }

  def bindTypes(op: AlgebraOp, variables: VariableTemplate) {
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
        throw new WQueryStaticCheckException("Variable $" + name + " cannot be bound")
    }
  }

  private def bindTypesFromRight(op: AlgebraOp, variables: VariableTemplate) {
    for ((name, pos) <- variables.rightVariablesIndexes) {
      if (op.maxTupleSize.map(pos < _).getOrElse(true))
        bindStepVariableType(name, op.rightType(pos))
      else
        throw new WQueryStaticCheckException("Variable $" + name + " cannot be bound")
    }
  }
}

object BindingsPattern {
  def apply() = new BindingsPattern
}