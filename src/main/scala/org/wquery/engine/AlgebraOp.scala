package org.wquery.engine

import org.wquery.model.{DataType, DataSet, WordNet}

abstract class AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings): DataSet
  def leftType(pos: Int): Set[DataType]
  def rightType(pos: Int): Set[DataType]
  def minTupleSize: Int
  def maxTupleSize: Option[Int]
  def bindingsPattern: BindingsPattern
  def referencedVariables: Set[Variable]
}
