package org.wquery.engine.planner

import collection.mutable.{Map, Set}
import scalaz._
import Scalaz._
import org.wquery.engine.operations._
import org.wquery.engine.{StepVariable, VariableTemplate, Variable}

class ConditionApplier(links: List[Link], conditions: Map[Option[Link], List[Condition]], context: BindingsSchema) {
  val appliedConditions = Set[Condition]()
  val pathVariables = links.map(_.variables.variables).asMA.sum
  val alreadyBoundVariables = Set[Variable]()

  def skipCondition(condition: Condition) = appliedConditions += condition

  def applyConditions(inputOp: AlgebraOp, currentLink: Link) = {
    var op = inputOp

    alreadyBoundVariables ++= currentLink.variables.variables
    val candidateConditions = (~conditions.get(none) ++ ~conditions.get(some(currentLink))).filterNot(appliedConditions.contains)

    for (condition <- candidateConditions) {
      if (condition.referencedVariables.forall{ variable =>
        alreadyBoundVariables.contains(variable) || (context.isBound(variable) && !pathVariables.contains(variable)) || variable === StepVariable.ContextVariable
      }) {
        appliedConditions += condition
        op = SelectOp(if (condition.referencedVariables.contains(StepVariable.ContextVariable)) BindOp(op, VariableTemplate(List(StepVariable.ContextVariable))) else op, condition)
      }
    }

    op
  }
}
