package org.wquery.lang.exprs

import org.wquery.model.WordNet
import org.wquery.lang.Context
import org.wquery.lang.operations.{AlgebraOp, BindingsSchema}

abstract class EvaluableExpr extends Expr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context): AlgebraOp
}

