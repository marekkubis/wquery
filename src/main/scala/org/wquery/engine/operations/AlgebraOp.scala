package org.wquery.engine.operations

import scalaz._
import Scalaz._
import org.wquery.model.{WordNetSchema, DataType, DataSet, WordNet}
import org.wquery.engine.{ProvidesTypes, ProvidesTupleSizes, Variable}

abstract class AlgebraOp extends ProvidesTypes with ProvidesTupleSizes {
  def evaluate(wordNet: WordNet, bindings: Bindings): DataSet
  def bindingsPattern: BindingsPattern
  def referencedVariables: Set[Variable]
  def referencesContext: Boolean
  def maxCount(wordNet: WordNetSchema): Option[BigInt]
}

object AlgebraOp {
  implicit val AlgebraOpEqual = equalA[AlgebraOp]
}