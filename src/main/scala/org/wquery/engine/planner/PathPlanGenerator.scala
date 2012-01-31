package org.wquery.engine.planner

import org.wquery.engine.operations._
import scalaz._
import Scalaz._
import org.wquery.model.WordNetSchema
import org.wquery.utils.BigIntOptionW._
import org.wquery.utils.BigIntOptionW

class PathPlanGenerator(path: Path) {
  def plan(wordNet: WordNetSchema, bindings: BindingsSchema) = {
    val seeds = if (!isTreePath(path)) {
      val seedSizeThreshold = some(BigInt(math.round(math.sqrt(wordNet.stats.domainSize.toDouble))))

      val (below, above) = path.links.zipWithIndex
        .map{ case (link, pos) => (link, pos, link.rightFringe.minBy(_._1.maxCount(wordNet))(BigIntOptionW.NoneMaxOrdering)._1.maxCount(wordNet)) }
        .partition(_._3 <= seedSizeThreshold)

      // return all seeds that have cost lower than or equal to threshold plus one seed above threshold
      (below ++ (if (above.isEmpty) Nil else List(above.minBy(_._3)))).map{ case (_, pos, _) => if (pos == 0) -1 else pos }
    } else {
      List(-1)
    }

    (for (seedSet <- seeds.toSet.subsets if seedSet.nonEmpty) yield {
      val applier = new ConditionApplier(path.links, path.conditions, bindings)
      val walkers = seedSet.toList.sorted.map(new PathWalker(wordNet, path.links, applier, _))

      walk(wordNet, bindings, walkers)
    }).toList
  }

  private def isTreePath(path: Path) = path.links.exists(link => link.isInstanceOf[PatternLink] && link.asInstanceOf[PatternLink].pos > 0)

  private def walk(wordNet: WordNetSchema, bindings: BindingsSchema, walkers: List[PathWalker]): AlgebraOp = {
    if (walkers.size == 1 && walkers.head.walkedEntirePath) {
      walkers.head.op
    } else {
      val (walker, walkerPos) = walkers.zipWithIndex.minBy(_._1.nextOp.maxCount(wordNet))

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
