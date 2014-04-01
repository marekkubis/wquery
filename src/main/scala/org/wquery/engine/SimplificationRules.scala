package org.wquery.engine

import scalaz._
import Scalaz._
import org.wquery.engine.operations._
import org.wquery.model.{SenseType, POSType, SynsetType, StringType}

object SimplificationRules {
  def applyConditionIfNotRedundant(op: AlgebraOp, condition: Condition) = {
    val isConditionRedundant = condition matchOrZero {
      case BinaryCondition("in", leftOp, rightOp) =>
        isRedundantStepVariableFilter(leftOp, rightOp) || isRedundantStepVariableFilter(rightOp, leftOp)
    }

    if (isConditionRedundant)
      op
    else
      SelectOp(op, condition)
  }

  private def isRedundantStepVariableFilter(variableOp: AlgebraOp, redundantOp: AlgebraOp) = {
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
