package org.wquery.lang.operations

import org.wquery.model.WordNet
import org.wquery.lang.Context

case class FunctionOp(function: Function, args: AlgebraOp) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = function.evaluate(args, wordNet, bindings, context)

  def leftType(pos: Int) = function.leftType(args, pos)

  def rightType(pos: Int) = function.rightType(args, pos)

  val minTupleSize = function.minTupleSize(args)

  val maxTupleSize = function.maxTupleSize(args)

  def bindingsPattern = function.bindingsPattern(args)

  val referencedVariables = args.referencedVariables

}


