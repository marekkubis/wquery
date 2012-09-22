package org.wquery.engine.planner

import collection.mutable.{Map, Set}
import scalaz._
import Scalaz._
import org.wquery.engine.operations._
import org.wquery.engine.{TupleVariable, StepVariable, VariableTemplate, Variable}
import org.wquery.model.{SenseType, POSType, SynsetType, StringType}

class ConditionApplier(links: List[Link], conditions: Map[Option[Link], List[Condition]], bindings: BindingsSchema) {
  val appliedConditions = Set[Condition]()
  val pathVariables = links.map(_.variables.variables).asMA.sum
  val alreadyBoundVariables = Set[Variable]()

  def prependContextIfReferenced(link: Link, algebraOp: AlgebraOp) = {
    if (hasContextDependentConditions(link))
      BindOp(algebraOp, VariableTemplate(List(StepVariable.ContextVariable, TupleVariable.Unnamed)))
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
        op = ConditionApplier.applyIfNotRedundant(op, condition)
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

object ConditionApplier {
  def applyIfNotRedundant(op: AlgebraOp, condition: Condition) = {
    val isConditionRedundant = condition matchOrZero {
      case BinaryCondition("in", leftOp, rightOp) =>
        iSRedundantStepVariableFilter(leftOp, rightOp) || iSRedundantStepVariableFilter(rightOp, leftOp)
    }

    if (isConditionRedundant)
      op
    else
      SelectOp(op, condition)
  }

  private def iSRedundantStepVariableFilter(variableOp: AlgebraOp, redundantOp: AlgebraOp) = {
    (variableOp, redundantOp) matchOrZero {
      case (StepVariableRefOp(_, types), FetchOp.synsets) if types == Set(SynsetType) =>
        true
      case (StepVariableRefOp(_, types), FetchOp.senses) if types == Set(SenseType) =>
        true
      case (StepVariableRefOp(_, types), FetchOp.words) if types == Set(StringType) =>
        true
      case (StepVariableRefOp(_, types), FetchOp.possyms) if types == Set(POSType) =>
        true
    }
  }
}