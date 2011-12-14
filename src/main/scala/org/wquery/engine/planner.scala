package org.wquery.engine

import org.wquery.model.WordNetSchema

object PathExprPlanner {

  def plan(steps: List[Step], wordNet: WordNetSchema, bindings: BindingsSchema) = {
    planForward(steps, wordNet, bindings)
  }

  private def planForward(steps: List[Step], wordNet: WordNetSchema, bindings: BindingsSchema) = {
    steps.tail.foldLeft(steps.head.asInstanceOf[NodeStep].generateOp)((op, step) => step.planForward(wordNet, bindings, op))
  }
}

sealed abstract class Node

sealed abstract class Step {
  def planForward(wordNet: WordNetSchema, bindings: BindingsSchema, op: AlgebraOp): AlgebraOp
}

case class RelationStep(pos: Int, pattern: RelationalPattern) extends Step {
  def planForward(wordNet: WordNetSchema, bindings: BindingsSchema, op: AlgebraOp) = {
    ExtendOp(op, pos, pattern)
  }
}

case class FilterStep(condition: Condition) extends Step {
  def planForward(wordNet: WordNetSchema, bindings: BindingsSchema, op: AlgebraOp) = {
    SelectOp(op, condition)
  }
}

case class NodeStep(generateOp: AlgebraOp) extends Step {
  def planForward(wordNet: WordNetSchema, bindings: BindingsSchema, op: AlgebraOp) = {
    SelectOp(op, BinaryCondition("in", ContextRefOp(0, op.rightType(0)), generateOp))
  }
}

case class ProjectStep(projectOp: AlgebraOp) extends Step {
  def planForward(wordNet: WordNetSchema, bindings: BindingsSchema, op: AlgebraOp) = {
    ProjectOp(op, projectOp)
  }
}

case class BindStep(variables: List[Variable]) extends Step with VariableTypeBindings {
  def planForward(wordNet: WordNetSchema, bindings: BindingsSchema, op: AlgebraOp) = {
    if (variables.isEmpty) {
      op
    } else {
      bindTypes(bindings, op, variables)
      BindOp(op, variables)
    }
  }
}
