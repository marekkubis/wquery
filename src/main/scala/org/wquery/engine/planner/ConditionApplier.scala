package org.wquery.engine.planner

import collection.mutable.{Map, Set}
import scalaz._
import Scalaz._
import org.wquery.engine.operations._
import org.wquery.engine.{PathVariable, StepVariable, VariableTemplate, Variable}

class ConditionApplier(links: List[Link], conditions: Map[Option[Link], List[Condition]], bindings: BindingsSchema) {
  val appliedConditions = Set[Condition]()
  val pathVariables = links.map(_.variables.variables).asMA.sum
  val alreadyBoundVariables = Set[Variable]()

  def prependContextIfReferenced(link: Link, algebraOp: AlgebraOp) = {
    if (hasContextDependentConditions(link))
      BindOp(algebraOp, VariableTemplate(List(StepVariable.ContextVariable, PathVariable.Unnamed)))
    else
      algebraOp
  }

  def appendContextIfReferenced(link: Link, algebraOp: AlgebraOp) = {
    if (hasContextDependentConditions(link))
      BindOp(algebraOp, VariableTemplate(List(StepVariable.ContextVariable)))
    else
      algebraOp
  }

  def skipCondition(condition: Condition) = appliedConditions += condition

  def applyConditions(inputOp: AlgebraOp, link: Link) = {
    var op = inputOp
    val linkConditions = (~conditions.get(none) ++ ~conditions.get(some(link))).filterNot(appliedConditions.contains)

    alreadyBoundVariables ++= link.variables.variables

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

  private def hasContextDependentConditions(link: Link) = {
    conditions.get(some(link)).orZero
      .filterNot(appliedConditions.contains(_))
      .exists(_.referencedVariables.contains(StepVariable.ContextVariable))
  }
}
