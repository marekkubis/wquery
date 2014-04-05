package org.wquery.lang.operations

import scalaz._
import Scalaz._
import org.wquery.model.{DataSet, WordNet}
import org.wquery.lang.Context

abstract class AlgebraOp extends ProvidesTypes with ProvidesTupleSizes with ReferencesVariables {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context): DataSet

  def bindingsPattern: BindingsPattern
}

object AlgebraOp {
  implicit val AlgebraOpEqual = equalA[AlgebraOp]
}
