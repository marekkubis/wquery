package org.wquery.engine.planner

import org.wquery.model.WordNetSchema
import org.wquery.engine.operations.Condition
import org.wquery.utils.BigIntOptionW
import scalaz._
import Scalaz._

class Seed(val wordNet: WordNetSchema, val links: List[Link], val pos: Int) {
  val (op, condition) = {
    val leftOps = (pos < links.size - 1) ?? List((links(pos + 1).leftFringe, none[Condition]))
    val rightOps = (pos >= 0) ?? links(pos).rightFringe

    (leftOps ++ rightOps).minBy(_._1.cost(wordNet))(BigIntOptionW.NoneMaxOrdering)
  }
}
