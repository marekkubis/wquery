package org.wquery.engine.planner

import org.wquery.engine.operations._
import scalaz._
import Scalaz._
import org.wquery.model.WordNetSchema

class PathPlanGenerator(path: Path) {
  def plan(wordNet: WordNetSchema, bindings: BindingsSchema) = {
    if (!isTreePath(path)) {
      val seeds = path.links.zipWithIndex
        .sortBy{ case (link, _) => link.rightFringe.minBy(_._1.maxCount(wordNet))._1.maxCount(wordNet) }
        .map{ case (_, pos) => if (pos == 0) -1 else pos }

      (for (seedSet <- seeds.slice(0, 2).toSet.subsets if seedSet.nonEmpty)
        yield walkFrom(wordNet, bindings, seedSet.toList.sorted)).toList
    } else {
      List(walkFrom(wordNet, bindings, List(-1)))
    }
  }

  private def isTreePath(path: Path) = path.links.exists(link => link.isInstanceOf[PatternLink] && link.asInstanceOf[PatternLink].pos > 0)

  private def walkFrom(wordNet: WordNetSchema, bindings: BindingsSchema, seeds: List[Int]) = {
    val applier = new ConditionApplier(path.links, path.conditions, bindings)
    val walkers = seeds.map(new PathWalker(wordNet, path.links, applier, _))

    walk(wordNet, bindings, walkers)
  }

  private def walk(wordNet: WordNetSchema, bindings: BindingsSchema, walkers: List[PathWalker]): AlgebraOp = {
    if (walkers.size == 1 && walkers.head.walkedEntirePath) {
      walkers.head.op
    } else {
      val walker = walkers.minBy(_.nextOp.maxCount(wordNet))
      val walkerPos = walkers.indexOf(walker)

      walker.step

      val newWalkers = if (walkerPos > 0 && walkers(walkerPos - 1).right == walker.left) {
        val leftWalker = walkers(walkerPos - 1)

        if (leftWalker.left != leftWalker.right)
          walker.op = NaturalJoinOp(leftWalker.op, walker.op)

        walker.left = leftWalker.left
        walkers.filterNot(_ == leftWalker)
      } else if (walkerPos < walkers.size - 1 && walkers(walkerPos + 1).left == walker.right) {
        val rightWalker = walkers(walkerPos + 1)

        if (rightWalker.left != rightWalker.right)
          walker.op = NaturalJoinOp(walker.op, rightWalker.op)

        walker.right = rightWalker.right
        walkers.filterNot(_ == rightWalker)
      } else {
        walkers
      }

      walk(wordNet, bindings, newWalkers)
    }
  }


}

class PathWalker(wordNet: WordNetSchema, links: List[Link], applier: ConditionApplier, pos: Int) {
  var op = {
    val leftOps = (pos < links.size - 1) ?? List((links(pos + 1).leftFringe, none[Condition]))
    val rightOps = (pos >= 0) ?? links(pos).rightFringe
    val (fringeOp, condition) = (leftOps ++ rightOps).minBy(_._1.maxCount(wordNet))

    condition.map(applier.skipCondition(_))
    fringeOp
  }

  var left = pos
  var right = pos

  def walkedEntirePath = left < 0 && right >= links.size - 1

  private def nextOps = {
    val backwardOp = (left >= 0) ?? some(links(left).backward(op))
    val forwardOp = (right < links.size - 1) ?? some(links(right + 1).forward(op))

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
