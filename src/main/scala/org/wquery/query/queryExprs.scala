package org.wquery.query.exprs

import org.wquery.lang._
import org.wquery.lang.exprs._
import org.wquery.lang.operations._
import org.wquery.model._
import org.wquery.query._
import org.wquery.query.operations._

case class EmissionExpr(expr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = EmitOp(expr.evaluationPlan(wordNet, bindings, context))
}

case class IteratorExpr(bindingExpr: EvaluableExpr, iteratedExpr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
    val bindingsOp = bindingExpr.evaluationPlan(wordNet, bindings, context)
    IterateOp(bindingsOp, iteratedExpr.evaluationPlan(wordNet, bindings union bindingsOp.bindingsPattern, context))
  }
}

case class IfElseExpr(conditionExpr: EvaluableExpr, ifExpr: EvaluableExpr, elseExpr: Option[EvaluableExpr]) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = IfElseOp(conditionExpr.evaluationPlan(wordNet, bindings, context),
    ifExpr.evaluationPlan(wordNet, bindings, context), elseExpr.map(_.evaluationPlan(wordNet, bindings, context)))
}

case class BlockExpr(exprs: List[EvaluableExpr]) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
    val blockBindings = BindingsSchema(bindings, true)

    BlockOp(exprs.map(expr => expr.evaluationPlan(wordNet, blockBindings, context)))
  }
}

case class WhileDoExpr(conditionExpr: EvaluableExpr, iteratedExpr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context)
    = WhileDoOp(conditionExpr.evaluationPlan(wordNet, bindings, context), iteratedExpr.evaluationPlan(wordNet, bindings, context))
}

case class FunctionDefinitionExpr(name: String, expr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
    bindings.bindSetVariableType(SetVariable.FunctionArgumentsVariable, FunctionDefinitionArgumentsRefOp(), 0, true)
    FunctionDefinitionOp(name, expr.evaluationPlan(wordNet, bindings, context))
  }
}

case class VariableAssignmentExpr(variables: List[SetVariable], expr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
    val op = expr.evaluationPlan(wordNet, bindings, context)

    bindings.bindSetVariableType(variables.last.name, op, variables.length - 1, true)

    for (pos <- 0 until variables.size - 1) {
      bindings.bindSetVariableType(variables(pos).name, op, pos, false)
    }

    AssignmentOp(variables, op)
  }
}

