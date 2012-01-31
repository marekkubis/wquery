package org.wquery.engine.planner

import org.wquery.model.WordNetSchema
import org.wquery.engine.operations.{AlgebraOp, Condition}
import scalaz._
import Scalaz._
import org.wquery.utils.BigIntOptionW

class PathWalker(wordNet: WordNetSchema, links: List[Link], applier: ConditionApplier, pos: Int) {
  var (op, seedCondition) = {
    val leftOps = (pos < links.size - 1) ?? List((links(pos + 1).leftFringe, none[Condition]))
    val rightOps = (pos >= 0) ?? links(pos).rightFringe

    (leftOps ++ rightOps).minBy(_._1.maxCount(wordNet))(BigIntOptionW.NoneMaxOrdering)
  }

  var left = pos
  var right = pos

  def walkedEntirePath = left < 0 && right >= links.size - 1

  private def nextOps = {
    val backwardOp = (left >= 0) ?? some(links(left).backward(applier.prependContextIfReferenced(links(left), op)))
    val forwardOp = (right < links.size - 1) ?? some(applier.appendContextIfReferenced(links(right + 1), links(right + 1).forward(op)))

    (backwardOp, forwardOp)
  }

  private def hasLowerMaxCount(leftOp: AlgebraOp, rightOp: AlgebraOp) = {
    (leftOp.maxCount(wordNet).some(backwardCount => rightOp.maxCount(wordNet).some(backwardCount < _).none(true)).none(false))
  }

  def nextOp = nextOps match {
    case (Some(backwardOp), Some(forwardOp)) =>
      if (hasLowerMaxCount(backwardOp, forwardOp)) backwardOp else forwardOp
    case (Some(backwardOp), None) =>
      backwardOp
    case (None, Some(forwardOp)) =>
      forwardOp
    case _ =>
      op
  }

  def step {
    seedCondition.map{ condition =>
      applier.skipCondition(condition)
      seedCondition = none
    }

    nextOps match {
      case (Some(backwardOp), Some(forwardOp)) =>
        if (hasLowerMaxCount(backwardOp, forwardOp)) {
          op = applier.applyConditions(backwardOp, links(left))
          left -= 1
        } else {
          op = applier.applyConditions(forwardOp, links(right + 1))
          right += 1
        }
      case (Some(backwardOp), None) =>
        op = applier.applyConditions(backwardOp, links(left))
        left -= 1
      case (None, Some(forwardOp)) =>
        op = applier.applyConditions(forwardOp, links(right + 1))
        right += 1
      case _ =>
        op
    }
  }
}
