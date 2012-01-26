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

      for (seed <- seeds.slice(0, 2))
        yield walkFrom(wordNet, bindings, seed)
    } else {
      List(walkFrom(wordNet, bindings, 1))
    }
  }

  private def isTreePath(path: Path) = path.links.exists(link => link.isInstanceOf[PatternLink] && link.asInstanceOf[PatternLink].pos > 0)

  private def walkFrom(wordNet: WordNetSchema, bindings: BindingsSchema, pos: Int) = {
    val applier = new ConditionApplier(path.links, path.conditions, bindings)
    val walker = new PathWalker(wordNet, path.links, applier, pos)

    while (walker.left >= 0 || walker.right < path.links.size - 1)
      walker.step

    walker.op
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

  def step {
    val backwardOp = (left >= 0) ?? some(links(left).backward(op))
    val forwardOp = (right < links.size - 1) ?? some(links(right + 1).forward(op))

    (backwardOp, forwardOp) match {
      case (Some(backwardOp), Some(forwardOp)) =>
        if ((backwardOp.maxCount(wordNet).some(backwardCount => forwardOp.maxCount(wordNet).some(backwardCount < _).none(true)).none(false))) {
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
