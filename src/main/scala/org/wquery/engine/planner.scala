package org.wquery.engine

import collection.mutable.{ListBuffer, Map}
import org.wquery.model.DataType

class LogicalPlanBuilder {
  val steps = new ListBuffer[Step]
  val bindings = Map[Step, List[Variable]]()
  
  def createStep(generator: AlgebraOp) {
    steps.append(NodeStep(generator.rightType(0), Some(generator)))
  }

  def appendStep(pos: Int, pattern: RelationalPattern) {
    steps.append(LinkStep(pos, pattern))
    steps.append(NodeStep(pattern.rightType(0), None)) // TODO use None or Some depending on type
  }

  def appendCondition(condition: Condition) {
    // if the filter is context dependednt then check if there is a suiatble variable
    //    if yes then substitute
    //    if not then add $__poz variable

    // build condition triggers
  }

  def appendVariables(variables: List[Variable]) {
    // TODO distinguish left and right traverses for step variables
    val step = if (steps.size == 1) steps.last else steps(steps.size - 2)
    bindings(step) = variables
  }

  def build = { // TODO return multiple plans - PlanEvaluator will choose one
    // travrse the graph biinding variables and firing trigers
    
    walkForward(0, steps.size - 1)
  }

  private def walkForward(leftPos: Int, rightPos: Int) = {
    val path = steps.slice(leftPos, rightPos + 1).toList

    var op = path.head.asInstanceOf[NodeStep].generator.get // TODO remove this cast and get

    for (step <- path.tail) {
      // bind variables

      step match {
        case LinkStep(pos, pattern) =>
          op = ExtendOp(op, pos, pattern, bindings.get(step).getOrElse(Nil))
        case _ =>
          // do nothing
      }

      // fetch triggers for variables bound in the current step
    }

    op
  }

}

sealed abstract class Step

case class NodeStep(types: Set[DataType], generator: Option[AlgebraOp]) extends Step

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
