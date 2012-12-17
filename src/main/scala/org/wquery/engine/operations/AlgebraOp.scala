package org.wquery.engine.operations

import scalaz._
import Scalaz._
import org.wquery.model.{DataSet, WordNet}
import org.wquery.engine._

abstract class AlgebraOp extends ProvidesTypes with ProvidesTupleSizes with ProvidesSize
with ReferencesVariables with HasCost {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context): DataSet

  def bindingsPattern: BindingsPattern
}

object AlgebraOp {
  implicit val AlgebraOpEqual = equalA[AlgebraOp]
}
