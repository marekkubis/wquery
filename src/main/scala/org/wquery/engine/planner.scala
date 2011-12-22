package org.wquery.engine

import collection.mutable.Stack
import org.wquery.model.DataType

class LogicalPlanBuilder {
  val steps = new Stack[Step]
  
  def createStep(generator: AlgebraOp) {
    steps.push(NodeStep(generator.rightType(0), Some(generator)))
  }

  def appendStep(pos: Int, pattern: RelationalPattern) {
    steps.push(LinkStep(pos, pattern))
    steps.push(NodeStep(pattern.rightType(0), None)) // TODO use None or Some depending on type
  }

  def appendCondition(condition: Condition) {


  }

  def appendVariables(variables: List[Variable]) {


  }

  def build = { // TODO return multiple plans - PlanEvaluator will choose one
    ConstantOp.empty
  }
}

sealed abstract class Step

case class NodeStep(types: Set[DataType], generator: Option[AlgebraOp]) extends Step {
//  def planForward(wordNet: WordNetSchema, bindings: BindingsSchema, op: AlgebraOp) = {
//    SelectOp(op, BinaryCondition("in", ContextRefOp(0, op.rightType(0)), generateOp))
//  }
}

case class LinkStep(pos: Int, pattern: RelationalPattern) extends Step {
//  def planForward(wordNet: WordNetSchema, bindings: BindingsSchema, op: AlgebraOp) = {
//    ExtendOp(op, pos, pattern)
//  }
}
//
//case class FilterStep(condition: Condition) extends Step {
//  def planForward(wordNet: WordNetSchema, bindings: BindingsSchema, op: AlgebraOp) = {
//    SelectOp(op, condition)
//  }
//}
//
//case class BindStep(variables: List[Variable]) extends Step with VariableTypeBindings {
//  def planForward(wordNet: WordNetSchema, bindings: BindingsSchema, op: AlgebraOp) = {
//    if (variables.isEmpty) {
//      op
//    } else {
//      bindTypes(bindings, op, variables)
//      BindOp(op, variables)
//    }
//  }
//}
