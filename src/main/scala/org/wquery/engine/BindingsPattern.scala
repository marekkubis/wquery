package org.wquery.engine

import scala.collection.mutable.Map
import org.wquery.model.DataType

class BindingsPattern {
  val stepVariablesTypes = Map[String, Set[DataType]]()
  val pathVariablesTypes = Map[String, (AlgebraOp, Int, Int)]()

  def bindStepVariableType(name: String, types: Set[DataType]) {
    stepVariablesTypes(name) = types
  }

  def bindPathVariableType(name: String, op: AlgebraOp, leftShift: Int, rightShift: Int) {
    pathVariablesTypes(name) = (op, leftShift, rightShift)
  }

  def lookupStepVariableType(name: String): Option[Set[DataType]] = stepVariablesTypes.get(name)

  def lookupPathVariableType(name: String): Option[(AlgebraOp, Int, Int)] = pathVariablesTypes.get(name)

  def union(that: BindingsPattern) = {
    val sum = BindingsPattern()

    sum.stepVariablesTypes ++= stepVariablesTypes
    sum.stepVariablesTypes ++= that.stepVariablesTypes
    sum.pathVariablesTypes ++= pathVariablesTypes
    sum.pathVariablesTypes ++= that.pathVariablesTypes

    sum
  }
}

object BindingsPattern {
  def apply() = new BindingsPattern
}