package org.wquery.update.exprs

import org.wquery.model._
import org.wquery._
import org.wquery.lang._
import org.wquery.lang.exprs._
import org.wquery.lang.operations._
import org.wquery.update._
import org.wquery.update.operations._

case class UpdateExpr(left: Option[EvaluableExpr], spec: RelationSpecification, op: String, right: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
    val leftOp = left.map(_.evaluationPlan(wordNet, bindings, context))
    val rightOp = right.evaluationPlan(wordNet, bindings, context)

    op match {
      case "+=" =>
        AddTuplesOp(leftOp, spec, rightOp)
      case "-=" =>
        RemoveTuplesOp(leftOp, spec, rightOp)
      case ":=" =>
        SetTuplesOp(leftOp, spec, rightOp)
    }
  }
}

case class MergeExpr(expr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
    val op = expr.evaluationPlan(wordNet, bindings, context)

    if (op.rightType(0).subsetOf(Set(SynsetType, SenseType))) {
      MergeOp(op)
    } else {
      throw new WQueryStaticCheckException("Merge operation requires synsets and/or senses as arguments")
    }
  }
}

