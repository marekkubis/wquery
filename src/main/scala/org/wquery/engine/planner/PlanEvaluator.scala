package org.wquery.engine.planner

import org.wquery.engine.operations.AlgebraOp
import org.wquery.model.WordNetSchema
import org.wquery.utils.Logging

object PlanEvaluator extends Logging {
  def chooseBest(wordNet: WordNetSchema, plans: List[AlgebraOp]) = {  
    debug(plans.map(p => (p, p.maxCount(wordNet))).toString)
    plans.minBy(_.maxCount(wordNet))  // TODO maxCost/maxMemorySize
  }
}