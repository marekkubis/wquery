package org.wquery.engine.planner

import collection.mutable.{Map, Set}
import org.wquery.engine.Variable
import scalaz._
import Scalaz._
import org.wquery.engine.operations.{SelectOp, AlgebraOp, BindingsSchema, Condition}

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
      if (condition.referencedVariables.forall { variable =>
        alreadyBoundVariables.contains(variable) || currentLink.variables.variables.contains(variable) || (context.isBound(variable) && !pathVariables.contains(variable))
      }) {
        appliedConditions += condition
        op = SelectOp(op, condition)
      }
    }

    op
  }
}
