package org.wquery.engine.operations

import org.wquery.WQueryEvaluationException
import org.wquery.model.{WQueryListOrdering, DataType, StringType, WordNet}
import org.wquery.engine.Variable

sealed abstract class Condition {
  def satisfied(wordNet: WordNet, bindings: Bindings): Boolean
  def referencedVariables: Set[Variable]
  def referencesContext: Boolean
}

case class OrCondition(exprs: List[Condition]) extends Condition {
  def satisfied(wordNet: WordNet, bindings: Bindings) = exprs.exists(_.satisfied(wordNet, bindings))

  def referencedVariables = exprs.foldLeft(Set.empty[Variable])((vars, expr) => vars union expr.referencedVariables)

  def referencesContext = exprs.exists(_.referencesContext)
}

case class AndCondition(exprs: List[Condition]) extends Condition {
  def satisfied(wordNet: WordNet, bindings: Bindings) = exprs.forall(_.satisfied(wordNet, bindings))

  def referencedVariables = exprs.foldLeft(Set.empty[Variable])((vars, expr) => vars union expr.referencedVariables)

  def referencesContext = exprs.exists(_.referencesContext)
}

case class NotCondition(expr: Condition) extends Condition {
  def satisfied(wordNet: WordNet, bindings: Bindings) = !expr.satisfied(wordNet, bindings)

  def referencedVariables = expr.referencedVariables

  def referencesContext = expr.referencesContext
}

case class BinaryCondition(op: String, leftOp: AlgebraOp, rightOp: AlgebraOp) extends Condition {
  var leftCache: (List[Any], Map[Any, List[Any]]) = null
  var rightCache: (List[Any], Map[Any, List[Any]]) = null
  val leftOpIsContextIndependent = leftOp.referencedVariables.isEmpty && !leftOp.referencesContext
  val rightOpIsContextIndependent = rightOp.referencedVariables.isEmpty && !rightOp.referencesContext

  def satisfied(wordNet: WordNet, bindings: Bindings) = {
    val (leftResult, leftGroup) = if (leftCache != null) {
      leftCache
    } else {
      val result = leftOp.evaluate(wordNet, bindings).paths.map(_.last)
      val group = result.groupBy(x => x)

      if (leftOpIsContextIndependent)
        leftCache = (result, group)

      (result, group)
    }

    val (rightResult, rightGroup) = if (rightCache != null) {
      rightCache
    } else {
      val result = rightOp.evaluate(wordNet, bindings).paths.map(_.last)
      val group = result.groupBy(x => x)

      if (rightOpIsContextIndependent)
        rightCache = (result, group)
      
      (result, group)
    }

    op match {
      case "=" =>
        leftGroup.forall{ case (left, leftValues) => rightGroup.get(left).map(rightValues => rightValues.size == leftValues.size).getOrElse(false) } &&
          rightGroup.forall{ case (right, rightValues) => leftGroup.get(right).map(leftValues => leftValues.size == rightValues.size).getOrElse(false) }
      case "!=" =>
        !(leftGroup.forall{ case (left, leftValues) => rightGroup.get(left).map(rightValues => rightValues.size == leftValues.size).getOrElse(false) } &&
            rightGroup.forall{ case (right, rightValues) => leftGroup.get(right).map(leftValues => leftValues.size == rightValues.size).getOrElse(false) })
      case "in" =>
        leftGroup.forall{ case (left, leftValues) => rightGroup.get(left).map(rightValues => leftValues.size <= rightValues.size).getOrElse(false) }
      case "pin" =>
        leftGroup.forall{ case (left, leftValues) => rightGroup.get(left).map(rightValues => leftValues.size <= rightValues.size).getOrElse(false) } && leftResult.size < rightResult.size
      case "=~" =>
        if (rightResult.size == 1) {
          // element context
          if (DataType.fromValue(rightResult.head) == StringType) {
            val regex = rightResult.head.asInstanceOf[String].r

            leftGroup.forall {
              case (elem: String, _) =>
                regex.findFirstIn(elem).isDefined
            }
          } else {
            throw new WQueryEvaluationException("The rightSet side of '" + op +
              "' should return exactly one character string value")
          }
        } else if (rightResult.isEmpty) {
          throw new WQueryEvaluationException("The rightSet side of '" + op + "' returns no values")
        } else { // rightResult.pathCount > 0
          throw new WQueryEvaluationException("The rightSet side of '" + op + "' returns more than one values")
        }
      case _ =>
        if (leftResult.size == 1 && rightResult.size == 1) {
          // element context
          op match {
            case "<=" =>
              WQueryListOrdering.lteq(leftResult, rightResult)
            case "<" =>
              WQueryListOrdering.lt(leftResult, rightResult)
            case ">=" =>
              WQueryListOrdering.gteq(leftResult, rightResult)
            case ">" =>
              WQueryListOrdering.gt(leftResult, rightResult)
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
  }

  def referencedVariables = leftOp.referencedVariables union rightOp.referencedVariables

  def referencesContext = leftOp.referencesContext || rightOp.referencesContext
}

case class RightFringeCondition(op: AlgebraOp) extends Condition {
  def satisfied(wordNet: WordNet, bindings: Bindings) = {
    val lastSteps = op.evaluate(wordNet, bindings).paths.map(_.last)

    !lastSteps.isEmpty && lastSteps.forall(x => x.isInstanceOf[Boolean] && x.asInstanceOf[Boolean])
  }

  def referencedVariables = op.referencedVariables

  def referencesContext = op.referencesContext
}
