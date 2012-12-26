package org.wquery.engine.planner

import org.wquery.engine.operations.AlgebraOp
import org.wquery.model.WordNet
import org.wquery.utils.Logging

object PlanEvaluator extends Logging {
  def chooseBest(wordNet: WordNet#Schema, plans: List[AlgebraOp]) = {
    debug(plans.map(p => (p, p.cost(wordNet))).toString)
    plans.minBy(_.cost(wordNet))
  }
}
