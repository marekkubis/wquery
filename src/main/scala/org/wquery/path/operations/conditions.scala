package org.wquery.path.operations

import org.wquery.WQueryEvaluationException
import org.wquery.lang._
import org.wquery.lang.operations._
import org.wquery.model._

import scalaz.Scalaz._

sealed abstract class Condition extends ReferencesVariables {
  def satisfied(wordNet: WordNet, bindings: Bindings, context: Context): Boolean
}

case class OrCondition(exprs: List[Condition]) extends Condition {
  def satisfied(wordNet: WordNet, bindings: Bindings, context: Context) = exprs.exists(_.satisfied(wordNet, bindings, context))

  val referencedVariables = exprs.foldLeft(Set.empty[Variable])((vars, expr) => vars union expr.referencedVariables)

}

case class AndCondition(exprs: List[Condition]) extends Condition {
  def satisfied(wordNet: WordNet, bindings: Bindings, context: Context) = exprs.forall(_.satisfied(wordNet, bindings, context))

  val referencedVariables = exprs.foldLeft(Set.empty[Variable])((vars, expr) => vars union expr.referencedVariables)

}

case class NotCondition(expr: Condition) extends Condition {
  def satisfied(wordNet: WordNet, bindings: Bindings, context: Context) = !expr.satisfied(wordNet, bindings, context)

  val referencedVariables = expr.referencedVariables

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
        setEqual(leftGroup, rightGroup)
      case "==" =>
        multiSetEqual(leftGroup, rightGroup)
      case "===" =>
        leftResult == rightResult
      case "!=" =>
        !setEqual(leftGroup, rightGroup)
      case "!==" =>
        !multiSetEqual(leftGroup, rightGroup)
      case "!===" =>
        leftResult != rightResult
      case "<<" =>
        setInclusion(leftGroup, rightGroup)
      case "<<<" =>
        multiSetInclusion(leftGroup, rightGroup)
      case "in" =>
        setInclusion(leftGroup, rightGroup)
      case "=~" =>
        val regexps = rightResult.collect{ case pattern: String => pattern.r }

        leftGroup.forall { leftElem => { (leftElem: @unchecked) match {
          case (elem: String, _) =>
            regexps.forall(_.findFirstIn(elem).isDefined)
          case _ =>
            false
        }}}
      case _ =>
        tryComparingAsSingletons(leftResult, rightResult)
    }
  }

  private def setInclusion(leftGroup: Map[Any, scala.List[Any]], rightGroup: Map[Any, scala.List[Any]]) = {
    leftGroup.forall{ case (left, leftValues) => rightGroup.contains(left) }
  }

  private def multiSetInclusion(leftGroup: Map[Any, scala.List[Any]], rightGroup: Map[Any, scala.List[Any]]) = {
    leftGroup.forall{ case (left, leftValues) => rightGroup.get(left).some(rightValues => leftValues.size <= rightValues.size).none(false) }
  }

  private def setEqual(leftGroup: Map[Any, scala.List[Any]], rightGroup: Map[Any, scala.List[Any]]) = {
    setInclusion(leftGroup, rightGroup) && setInclusion(rightGroup, leftGroup)
  }

  private def multiSetEqual(leftGroup: Map[Any, scala.List[Any]], rightGroup: Map[Any, scala.List[Any]]) = {
    multiSetInclusion(leftGroup, rightGroup) && multiSetInclusion(rightGroup, leftGroup)
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

}

case class RightFringeCondition(op: AlgebraOp) extends Condition {
  def satisfied(wordNet: WordNet, bindings: Bindings, context: Context) = {
    val lastSteps = op.evaluate(wordNet, bindings, context).paths.map(_.last)

    !lastSteps.isEmpty && lastSteps.forall(x => x.isInstanceOf[Boolean] && x.asInstanceOf[Boolean])
  }

  val referencedVariables = op.referencedVariables

}
