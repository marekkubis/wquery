package org.wquery.engine.operations

import org.wquery.model.{DataType, DataSet, WordNet}
import org.wquery.engine.{Variable, BindingsPattern}

abstract class AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings): DataSet
  def leftType(pos: Int): Set[DataType]
  def rightType(pos: Int): Set[DataType]
  def minTupleSize: Int
  def maxTupleSize: Option[Int]
  def bindingsPattern: BindingsPattern
  def referencedVariables: Set[Variable]
  def referencesContext: Boolean
}
