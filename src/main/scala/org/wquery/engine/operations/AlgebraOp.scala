package org.wquery.engine.operations

import scalaz._
import Scalaz._
import org.wquery.model.{WordNetSchema, DataSet, WordNet}
import org.wquery.engine.{ContainsReferences, ProvidesTypes, ProvidesTupleSizes}

abstract class AlgebraOp extends ProvidesTypes with ProvidesTupleSizes with ContainsReferences {
  def evaluate(wordNet: WordNet, bindings: Bindings): DataSet
  def bindingsPattern: BindingsPattern
  def maxCount(wordNet: WordNetSchema): Option[BigInt]
}

object AlgebraOp {
  implicit val AlgebraOpEqual = equalA[AlgebraOp]
}