package org.wquery.engine.operations

import org.wquery.WQueryEvaluationException
import org.wquery.engine.{Context, ReferencesVariables, Variable}
import org.wquery.model._
import scalaz._
import Scalaz._
import org.wquery.utils.BigIntOptionW._

sealed abstract class Condition extends ReferencesVariables with HasCost {
  def satisfied(wordNet: WordNet, bindings: Bindings, context: Context): Boolean
  def selectivity(wordNet: WordNet#Schema): Double
}

case class OrCondition(exprs: List[Condition]) extends Condition {
  def satisfied(wordNet: WordNet, bindings: Bindings, context: Context) = exprs.exists(_.satisfied(wordNet, bindings, context))

  val referencedVariables = exprs.foldLeft(Set.empty[Variable])((vars, expr) => vars union expr.referencedVariables)

  def cost(wordNet: WordNet#Schema) = exprs.map(_.cost(wordNet)).sequence.map(_.sum)

  def selectivity(wordNet: WordNet#Schema) = exprs.map(_.selectivity(wordNet)).max
}

case class AndCondition(exprs: List[Condition]) extends Condition {
  def satisfied(wordNet: WordNet, bindings: Bindings, context: Context) = exprs.forall(_.satisfied(wordNet, bindings, context))

  val referencedVariables = exprs.foldLeft(Set.empty[Variable])((vars, expr) => vars union expr.referencedVariables)

  def cost(wordNet: WordNet#Schema) = exprs.map(_.cost(wordNet)).sequence.map(_.sum)

  def selectivity(wordNet: WordNet#Schema) = exprs.map(_.selectivity(wordNet)).min
}

case class NotCondition(expr: Condition) extends Condition {
  def satisfied(wordNet: WordNet, bindings: Bindings, context: Context) = !expr.satisfied(wordNet, bindings, context)

  val referencedVariables = expr.referencedVariables

  def cost(wordNet: WordNet#Schema) = expr.cost(wordNet)

  def selectivity(wordNet: WordNet#Schema) = 1 - expr.selectivity(wordNet)
}

case class BinaryCondition(op: String, leftOp: AlgebraOp, rightOp: AlgebraOp) extends Condition {
  var leftCache = none[(List[Any], Map[Any, List[Any]])]
  var rightCache = none[(List[Any], Map[Any, List[Any]])]

  def satisfied(wordNet: WordNet, bindings: Bindings, context: Context) = {
    val (leftResult, leftGroup) = leftCache|{
      val result = leftOp.evaluate(wordNet, bindings, context).paths.map(_.last)
      val group = result.groupBy(x => x)

      if (leftOp.referencedVariables.isEmpty)
        leftCache = some((result, group))

      (result, group)
    }

    val (rightResult, rightGroup) = rightCache|{
      val result = rightOp.evaluate(wordNet, bindings, context).paths.map(_.last)
      val group = result.groupBy(x => x)

      if (rightOp.referencedVariables.isEmpty)
        rightCache = some((result, group))

      (result, group)
    }

    op match {
      case "=" =>
        leftGroup.forall{ case (left, leftValues) => rightGroup.get(left).some(rightValues => rightValues.size == leftValues.size).none(false) } &&
          rightGroup.forall{ case (right, rightValues) => leftGroup.get(right).some(leftValues => leftValues.size == rightValues.size).none(false) }
      case "!=" =>
        !(leftGroup.forall{ case (left, leftValues) => rightGroup.get(left).some(rightValues => rightValues.size == leftValues.size).none(false) } &&
            rightGroup.forall{ case (right, rightValues) => leftGroup.get(right).some(leftValues => leftValues.size == rightValues.size).none(false) })
      case "in" =>
        leftGroup.forall{ case (left, leftValues) => rightGroup.get(left).some(rightValues => leftValues.size <= rightValues.size).none(false) }
      case "pin" =>
        leftGroup.forall{
          case (left, leftValues) => rightGroup.get(left).some(rightValues => leftValues.size <= rightValues.size).none(false)
        } && leftResult.size < rightResult.size
      case "=~" =>
        val regexps = rightResult.collect{ case pattern: String => pattern.r }

        leftGroup.forall { leftElem => { (leftElem: @unchecked) match {
          case (elem: String, _) =>
            regexps.forall(_.findFirstIn(elem).isDefined)
        }}}
      case _ =>
        tryComparingAsSingletons(leftResult, rightResult)
    }
  }

  private def tryComparingAsSingletons(leftResult: List[Any], rightResult: List[Any]) = {
    if (leftResult.size == 1 && rightResult.size == 1) {
      // element context
      op match {
        case "<=" =>
          AnyListOrdering.lteq(leftResult, rightResult)
        case "<" =>
          AnyListOrdering.lt(leftResult, rightResult)
        case ">=" =>
          AnyListOrdering.gteq(leftResult, rightResult)
        case ">" =>
          AnyListOrdering.gt(leftResult, rightResult)
      }
    } else {
      if (leftResult.isEmpty)
        throw new WQueryEvaluationException("The leftSet side of '" + op + "' returns no values")
      if (leftResult.size > 1)
        throw new WQueryEvaluationException("The leftSet side of '" + op + "' returns more than one value")
      if (rightResult.isEmpty)
        throw new WQueryEvaluationException("The rightSet side of '" + op + "' returns no values")
      if (rightResult.size > 1)
        throw new WQueryEvaluationException("The rightSet side of '" + op + "' returns more than one values")

      // the following shall not happen
      throw new WQueryEvaluationException("Both sides of '" + op + "' should return exactly one value")
    }
  }

  val referencedVariables = leftOp.referencedVariables union rightOp.referencedVariables

  def cost(wordNet: WordNet#Schema) = (leftOp.cost(wordNet) + rightOp.cost(wordNet))*some(2) // op cost + grouping cost

  def selectivity(wordNet: WordNet#Schema) = 0.5 //TODO extend estimation

}

case class RightFringeCondition(op: AlgebraOp) extends Condition {
  def satisfied(wordNet: WordNet, bindings: Bindings, context: Context) = {
    val lastSteps = op.evaluate(wordNet, bindings, context).paths.map(_.last)

    !lastSteps.isEmpty && lastSteps.forall(x => x.isInstanceOf[Boolean] && x.asInstanceOf[Boolean])
  }

  val referencedVariables = op.referencedVariables

  def cost(wordNet: WordNet#Schema) = op.cost(wordNet)

  def selectivity(wordNet: WordNet#Schema) = 0.5
}
