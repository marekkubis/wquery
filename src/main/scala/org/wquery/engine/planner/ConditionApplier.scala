package org.wquery.engine.planner

import collection.mutable.{Map, Set}
import scalaz._
import Scalaz._
import org.wquery.engine.operations._
import org.wquery.model.{Forward, Direction}
import org.wquery.engine.{StepVariable, VariableTemplate, Variable}

class ConditionApplier(links: List[Link], conditions: Map[Option[Link], List[Condition]], bindings: BindingsSchema) {
  val appliedConditions = Set[Condition]()
  val pathVariables = links.map(_.variables.variables).asMA.sum
  val alreadyBoundVariables = Set[Variable]()

  def skipCondition(condition: Condition) = appliedConditions += condition

  def hasContextDependentConditions(link: Link) = {
    conditions.get(some(link)).orZero
      .filterNot(appliedConditions.contains(_))
      .exists(_.referencedVariables.contains(StepVariable.ContextVariable))
  }

  def applyConditions(inputOp: AlgebraOp, link: Link, direction: Direction) = {
    var op = inputOp
    val linkConditions = (~conditions.get(none) ++ ~conditions.get(some(link))).filterNot(appliedConditions.contains)

    alreadyBoundVariables ++= link.variables.variables

    if (hasContextDependentConditions(link) && direction == Forward)
      op = BindOp(op, VariableTemplate(List(StepVariable.ContextVariable)))

    for (condition <- linkConditions) {
      if (condition.referencedVariables.forall{ variable =>
        alreadyBoundVariables.contains(variable) || (bindings.isBound(variable) && !pathVariables.contains(variable)) || variable === StepVariable.ContextVariable
      }) {
        appliedConditions += condition
        op = SelectOp(op, condition)
      }
    }

    op
  }
}
