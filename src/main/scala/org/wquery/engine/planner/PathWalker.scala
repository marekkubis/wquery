package org.wquery.engine.planner

import org.wquery.engine.operations.AlgebraOp
import scalaz._
import Scalaz._

class PathWalker(seed: Seed, applier: ConditionApplier) {
  var left = seed.pos
  var right = seed.pos
  var op = seed.op
  var seedCondition = seed.condition

  def walkedEntirePath = left < 0 && right >= seed.links.size - 1

  private def nextOps = {
    val backwardOp = (left >= 0) ?? some(seed.links(left).backward(applier.prependContextIfReferenced(seed.links(left), op)))
    val forwardOp = (right < seed.links.size - 1) ?? some(applier.appendContextIfReferenced(seed.links(right + 1), seed.links(right + 1).forward(op)))

    (backwardOp, forwardOp)
  }

  private def hasLowerMaxCount(leftOp: AlgebraOp, rightOp: AlgebraOp) = {
    (leftOp.maxCount(seed.wordNet).some(backwardCount => rightOp.maxCount(seed.wordNet).some(backwardCount < _).none(true)).none(false))
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
          op = applier.applyConditions(backwardOp, seed.links(left))
          left -= 1
        } else {
          op = applier.applyConditions(forwardOp, seed.links(right + 1))
          right += 1
        }
      case (Some(backwardOp), None) =>
        op = applier.applyConditions(backwardOp, seed.links(left))
        left -= 1
      case (None, Some(forwardOp)) =>
        op = applier.applyConditions(forwardOp, seed.links(right + 1))
        right += 1
      case _ =>
        op
    }
  }
}
