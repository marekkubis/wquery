// scalastyle:off file.size.limit
// scalastyle:off number.of.types

package org.wquery.query.operations

import scalaz._
import Scalaz._
import org.wquery.model._
import org.wquery.lang._
import org.wquery.lang.operations._
import org.wquery.query._
import org.wquery.utils.BigIntOptionW._
import org.wquery.utils.IntOptionW._

sealed abstract class QueryOp extends AlgebraOp

case class EmitOp(op: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = op.evaluate(wordNet, bindings, context)

  def leftType(pos: Int) = op.leftType(pos)

  def rightType(pos: Int) = op.rightType(pos)

  val minTupleSize = op.minTupleSize

  val maxTupleSize = op.maxTupleSize

  def bindingsPattern = op.bindingsPattern

  val referencedVariables = op.referencedVariables

}

case class IterateOp(bindingOp: AlgebraOp, iteratedOp: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = {
    val bindingSet = bindingOp.evaluate(wordNet, bindings, context)
    val buffer = new DataSetBuffer
    val pathVarNames = bindingSet.pathVars.keys.toSeq
    val stepVarNames = bindingSet.stepVars.keys.toSeq

    for (i <- 0 until bindingSet.pathCount) {
      val tuple = bindingSet.paths(i)

      pathVarNames.foreach { pathVar =>
        val varPos = bindingSet.pathVars(pathVar)(i)
        bindings.bindTupleVariable(pathVar, tuple.slice(varPos._1, varPos._2))
      }

      stepVarNames.foreach(stepVar => bindings.bindStepVariable(stepVar, tuple(bindingSet.stepVars(stepVar)(i))))
      buffer.append(iteratedOp.evaluate(wordNet, bindings, context))
    }

    buffer.toDataSet
  }

  def leftType(pos: Int) = iteratedOp.leftType(pos)

  def rightType(pos: Int) = iteratedOp.rightType(pos)

  val minTupleSize = iteratedOp.minTupleSize

  val maxTupleSize = iteratedOp.maxTupleSize

  def bindingsPattern = iteratedOp.bindingsPattern

  val referencedVariables = (iteratedOp.referencedVariables -- bindingOp.bindingsPattern.variables) ++ bindingOp.referencedVariables

}

case class IfElseOp(conditionOp: AlgebraOp, ifOp: AlgebraOp, elseOp: Option[AlgebraOp]) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = {
    if (conditionOp.evaluate(wordNet, bindings, context).isTrue)
      ifOp.evaluate(wordNet, bindings, context)
    else
      elseOp.map(_.evaluate(wordNet, bindings, context)).orZero
  }

  def leftType(pos: Int) = ifOp.leftType(pos) ++ elseOp.map(_.leftType(pos)).orZero

  def rightType(pos: Int) = ifOp.rightType(pos) ++ elseOp.map(_.rightType(pos)).orZero

  val minTupleSize = elseOp.some(_.minTupleSize min ifOp.minTupleSize).none(ifOp.minTupleSize)

  val maxTupleSize = elseOp.some(_.maxTupleSize max ifOp.maxTupleSize).none(ifOp.maxTupleSize)

  def bindingsPattern = elseOp.some(_.bindingsPattern union ifOp.bindingsPattern).none(ifOp.bindingsPattern)

  val referencedVariables = conditionOp.referencedVariables ++ ifOp.referencedVariables ++ elseOp.map(_.referencedVariables).orZero

}

case class BlockOp(ops: List[AlgebraOp]) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = {
    val blockBindings = Bindings(bindings, true)
    val buffer = new DataSetBuffer

    for (op <- ops)
      buffer.append(op.evaluate(wordNet, blockBindings, context))

    buffer.toDataSet
  }

  def leftType(pos: Int) = ops.map(_.leftType(pos)).flatten.toSet

  def rightType(pos: Int) = ops.map(_.rightType(pos)).flatten.toSet

  val minTupleSize = ops.map(_.minTupleSize).min

  val maxTupleSize = ops.map(_.maxTupleSize).sequence.map(_.sum)

  def bindingsPattern = {
    // It is assumed that all statements in a block provide same binding schemas
    ops.headOption.some(_.bindingsPattern).none(BindingsPattern())
  }

  val referencedVariables = ops.map(_.referencedVariables).asMA.sum

}

case class AssignmentOp(variable: SetVariable, op: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = {
    val result = op.evaluate(wordNet, bindings, context)
    bindings.bindSetVariable(variable.name, result)
    DataSet.empty
  }

  def leftType(pos: Int) = Set.empty

  def rightType(pos: Int) = Set.empty

  val minTupleSize = 0

  val maxTupleSize = some(0)

  def bindingsPattern = BindingsPattern()

  val referencedVariables = op.referencedVariables

}

case class WhileDoOp(conditionOp: AlgebraOp, iteratedOp: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = {
    val buffer = new DataSetBuffer

    while (conditionOp.evaluate(wordNet, bindings, context).isTrue)
      buffer.append(iteratedOp.evaluate(wordNet, bindings, context))

    buffer.toDataSet
  }

  def leftType(pos: Int) = iteratedOp.leftType(pos)

  def rightType(pos: Int) = iteratedOp.rightType(pos)

  val minTupleSize = iteratedOp.minTupleSize

  val maxTupleSize = iteratedOp.maxTupleSize

  def bindingsPattern = iteratedOp.bindingsPattern

  val referencedVariables = conditionOp.referencedVariables ++ iteratedOp.referencedVariables

}

