package org.wquery.engine

import org.wquery.WQueryEvaluationException
import org.wquery.model.{WQueryListOrdering, DataType, StringType, WordNet}

sealed abstract class Condition {
  def satisfied(wordNet: WordNet, bindings: Bindings): Boolean
}

case class OrCondition(exprs: List[Condition]) extends Condition {
  def satisfied(wordNet: WordNet, bindings: Bindings) = exprs.exists(_.satisfied(wordNet, bindings))
}

case class AndCondition(exprs: List[Condition]) extends Condition {
  def satisfied(wordNet: WordNet, bindings: Bindings) = exprs.forall(_.satisfied(wordNet, bindings))
}

case class NotCondition(expr: Condition) extends Condition {
  def satisfied(wordNet: WordNet, bindings: Bindings) = !expr.satisfied(wordNet, bindings)
}

case class BinaryCondition(op: String, leftOp: AlgebraOp, rightOp: AlgebraOp) extends Condition {
  def satisfied(wordNet: WordNet, bindings: Bindings) = {
    val leftResult = leftOp.evaluate(wordNet, bindings).paths.map(_.last)
    val rightResult = rightOp.evaluate(wordNet, bindings).paths.map(_.last)

    op match {
      case "=" =>
        leftResult.forall(rightResult.contains) && rightResult.forall(leftResult.contains)
      case "!=" =>
        !(leftResult.forall(rightResult.contains) && rightResult.forall(leftResult.contains))
      case "in" =>
        leftResult.forall(rightResult.contains)
      case "pin" =>
        leftResult.forall(rightResult.contains) && leftResult.size < rightResult.size
      case "=~" =>
        if (rightResult.size == 1 ) {
          // element context
          if (DataType(rightResult.head) == StringType) {
            val regex = rightResult.head.asInstanceOf[String].r

            leftResult.forall {
              case elem: String =>
                regex.findFirstIn(elem).map(_ => true).getOrElse(false)
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
}

case class RightFringeCondition(op: AlgebraOp) extends Condition {
  def satisfied(wordNet: WordNet, bindings: Bindings) = {
    val lastSteps = op.evaluate(wordNet, bindings).paths.map(_.last)

    !lastSteps.isEmpty && lastSteps.forall(x => x.isInstanceOf[Boolean] && x.asInstanceOf[Boolean])
  }
}
