package org.wquery.engine.operations

import scalaz._
import Scalaz._
import org.wquery.engine.Variable
import org.wquery.model.{WordNetSchema, DataType, DataSet, WordNet}

abstract class AlgebraOp extends ProvidesTupleSizes {
  def evaluate(wordNet: WordNet, bindings: Bindings): DataSet
  def leftType(pos: Int): Set[DataType]
  def rightType(pos: Int): Set[DataType]
  def bindingsPattern: BindingsPattern
  def referencedVariables: Set[Variable]
  def referencesContext: Boolean
  def maxCount(wordNet: WordNetSchema): Option[BigInt]
}

object AlgebraOp {
  implicit val AlgebraOpEqual = equalA[AlgebraOp]
}