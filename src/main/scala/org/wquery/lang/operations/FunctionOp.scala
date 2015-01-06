package org.wquery.lang.operations

import org.wquery.lang.Context
import org.wquery.model.WordNet

import scalaz.Scalaz._
import scalaz._

case class FunctionOp(function: Function, args: Option[AlgebraOp]) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = function.evaluate(args, wordNet, bindings, context)

  def leftType(pos: Int) = function.leftType(args, pos)

  def rightType(pos: Int) = function.rightType(args, pos)

  val minTupleSize = function.minTupleSize(args)

  val maxTupleSize = function.maxTupleSize(args)

  def bindingsPattern = function.bindingsPattern(args)

  val referencedVariables = args.map(_.referencedVariables).orZero

}


