package org.wquery.engine

import collection.mutable.ListBuffer
import org.wquery.model.WordNetSchema

object PathExprPlanner {

  def plan(steps: List[StepExpr], wordNet: WordNetSchema, bindings: BindingsSchema) = {
    // TODO infer node exprs
    planFromLeft(steps, wordNet, bindings)
  }

  private def planFromLeft(exprs: List[StepExpr], wordNet: WordNetSchema, bindings: BindingsSchema) = {
    val unappliedFilters = new ListBuffer[FilterStepExpr]
    val unappliedProjections = new ListBuffer[ProjectionStepExpr]
    val steps = new ListBuffer[StepExpr]

    exprs.map {
      case trans: FilterStepExpr =>
        unappliedFilters.append(trans)
        steps.append(trans)
      case trans: ProjectionStepExpr =>
        unappliedProjections.append(trans)
        steps.append(trans)
      case trans =>
        steps.append(trans)
    }

    val headPlan = steps.head.asInstanceOf[NodeStepExpr].generatePlan(wordNet, bindings)
    steps.tail.foldLeft(headPlan)((prevPlan, step) => step.transformPlan(wordNet, bindings, prevPlan))

    // evaluate step
      // if binds new variables
        // check if triggers filter
        // check if triggers projection
  }
}
