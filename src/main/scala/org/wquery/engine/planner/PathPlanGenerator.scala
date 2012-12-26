package org.wquery.engine.planner

import org.wquery.engine.operations._
import scalaz._
import Scalaz._
import org.wquery.model.WordNet
import org.wquery.utils.BigIntOptionW._
import org.wquery.utils.BigIntOptionW

class PathPlanGenerator(path: Path) {
  def plan(wordNet: WordNet#Schema, bindings: BindingsSchema) = {
    val seeds = (-1::(1 until path.links.size).toList).map(new Seed(wordNet, path.links, _))
    val seedSizeThreshold = some(BigInt(math.round(math.sqrt(wordNet.stats.domainSize.toDouble))))
    val (below, above) = seeds.partition(_.op.cost(wordNet) <= seedSizeThreshold)
    val selectedSeeds = (below ++ (if (above.isEmpty) Nil else List(above.minBy(_.op.cost(wordNet))(BigIntOptionW.NoneMaxOrdering))))

    (for (seedSet <- selectedSeeds.toSet.subsets if seedSet.nonEmpty) yield {
      val applier = new ConditionApplier(path.links, path.conditions, bindings)
      val walkers = seedSet.toList.sortBy(_.pos).map(new PathWalker(_, applier))

      walk(wordNet, bindings, walkers)
    }).toList
  }

  private def walk(wordNet: WordNet#Schema, bindings: BindingsSchema, walkers: List[PathWalker]): AlgebraOp = {
    if (walkers.size == 1 && walkers.head.walkedEntirePath) {
      walkers.head.op
    } else {
      val (walker, walkerPos) = walkers.zipWithIndex.minBy(_._1.nextOp.cost(wordNet))(BigIntOptionW.NoneMaxOrdering)

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
