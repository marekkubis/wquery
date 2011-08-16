package org.wquery.engine

import org.wquery.model.WordNetSchema

object PathExprPlanner {

  def plan(steps: List[Step], wordNet: WordNetSchema, bindings: BindingsSchema) = {
    // TODO infer node exprs
    planForward(steps, wordNet, bindings)
  }

  private def planForward(steps: List[Step], wordNet: WordNetSchema, bindings: BindingsSchema) = {
    steps.tail.foldLeft(steps.head.asInstanceOf[NodeStep].generateOp)((op, step) => step.planForward(wordNet, bindings, op))
  }
}

sealed abstract class Node

sealed abstract class Step(variables: List[Variable]) extends  VariableTypeBindings {
  def bind(wordNet: WordNetSchema, bindings: BindingsSchema, op: AlgebraOp) = {
    if (variables.isEmpty) {
      op
    } else {
      bindTypes(bindings, op, variables)
      BindOp(op, variables)
    }
  }

  def planForward(wordNet: WordNetSchema, bindings: BindingsSchema, op: AlgebraOp): AlgebraOp
}

case class RelationStep(pos: Int, pattern: RelationalPattern, variables: List[Variable]) extends Step(variables) {
  def planForward(wordNet: WordNetSchema, bindings: BindingsSchema, op: AlgebraOp) = {
    bind(wordNet, bindings, ExtendOp(op, pos, pattern))
  }
}

case class FilterStep(condition: Condition, variables: List[Variable]) extends Step(variables) {
  def planForward(wordNet: WordNetSchema, bindings: BindingsSchema, op: AlgebraOp) = {
    val filterBindings = BindingsSchema(bindings, false)
    filterBindings.bindContextOp(op)
    bind(wordNet, bindings, SelectOp(op, condition))
  }
}

case class NodeStep(generateOp: AlgebraOp, variables: List[Variable]) extends Step(variables) {
  def planForward(wordNet: WordNetSchema, bindings: BindingsSchema, op: AlgebraOp) = {
    val filterBindings = BindingsSchema(bindings, false)
    filterBindings.bindContextOp(op)
    bind(wordNet, bindings, SelectOp(op, BinaryCondition("in", ContextRefOp(0, op.rightType(0)), generateOp)))
  }
}

case class ProjectStep(projectOp: AlgebraOp, variables: List[Variable]) extends Step(variables) {
  def planForward(wordNet: WordNetSchema, bindings: BindingsSchema, op: AlgebraOp) = {
    bind(wordNet, bindings, ProjectOp(op, projectOp))
  }
}
