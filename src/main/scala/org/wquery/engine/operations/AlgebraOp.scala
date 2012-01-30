package org.wquery.engine.operations

import scalaz._
import Scalaz._
import org.wquery.model.{WordNetSchema, DataSet, WordNet}
import org.wquery.engine.{ProvidesSize, ReferencesVariables, ProvidesTypes, ProvidesTupleSizes}

abstract class AlgebraOp extends ProvidesTypes with ProvidesTupleSizes with ProvidesSize with ReferencesVariables {
  def evaluate(wordNet: WordNet, bindings: Bindings): DataSet
  def bindingsPattern: BindingsPattern
  def cost(wordNet: WordNetSchema): Option[BigInt]
}

object AlgebraOp {
  implicit val AlgebraOpEqual = equalA[AlgebraOp]
}