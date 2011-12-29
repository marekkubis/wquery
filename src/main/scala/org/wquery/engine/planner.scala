package org.wquery.engine

import collection.mutable.{ListBuffer, Map}
import scalaz._
import Scalaz._
import org.wquery.model.DataType

class LogicalPlanBuilder(context: BindingsSchema) {
  val steps = new ListBuffer[Step]
  val bindings = Map[Step, VariableTemplate]()
  val conditions = new ListBuffer[(Option[Step], Condition)]
  
  def createStep(generator: AlgebraOp) {
    steps.append(new NodeStep(generator.rightType(0), some(generator)))
  }

  def appendStep(pos: Int, pattern: RelationalPattern) {
    steps.append(new LinkStep(pos, pattern))
    steps.append(new NodeStep(pattern.rightType(0), none)) // TODO backwardGenerator: use None or Some depending on type
  }

  def appendCondition(condition: Condition) {    
    // if the filter is context dependednt then check if there is a suiatble variable
    //    if yes then substitute
    //    if not then add $__poz variable

    // build condition trigger
    
    conditions.append((steps.lastOption, condition))
  }

  def appendVariables(variables: VariableTemplate) {
    // TODO distinguish left and right traverses for step variables
    val step = if (steps.size == 1) steps.last else steps(steps.size - 2)
    bindings(step) = variables
  }

  def build = { // TODO return multiple plans - PlanEvaluator will choose one
    // travrse the graph
    
    walkForward(0, steps.size - 1)
  }

  private def walkForward(leftPos: Int, rightPos: Int) = {
    val path = steps.slice(leftPos, rightPos + 1).toList
    val applier = new ConditionApplier(conditions.toList, context)

    var op: AlgebraOp = path.head.asInstanceOf[NodeStep].generator.get // TODO remove this cast and get
    op = bindings.get(path.head).map(template => BindOp(op, template)).getOrElse(op)
    op = applier.applyConditions(op, path.head, bindings.get(path.head).getOrElse(VariableTemplate.empty))

    for (step <- path.tail) {
      val stepBindings = bindings.get(step).getOrElse(VariableTemplate.empty)

      step match {
        case link: LinkStep =>
          op = ExtendOp(op, link.pos, link.pattern, stepBindings)
        case _ =>
          // do nothing
      }

      op = applier.applyConditions(op, step, stepBindings)
    }

    op
  }
}

class ConditionApplier(conditions: List[(Option[Step], Condition)], context: BindingsSchema) {
  val appliedConditions = scala.collection.mutable.Set[Condition]()
  val alreadyBoundVariables = scala.collection.mutable.Set[Variable]()

  def applyConditions(inputOp: AlgebraOp, currentStep: Step, template: VariableTemplate) = {
    var op = inputOp
    alreadyBoundVariables ++= template.variables

    for ((step, condition) <- conditions.filterNot{ case (_, c) => appliedConditions.contains(c)}) {
      if (condition.referencedVariables.forall { variable =>
        alreadyBoundVariables.contains(variable) || template.variables.contains(variable) || context.isBound(variable)
      } && (!condition.referencesContext || step.some(_ == currentStep).none(false))) {
        appliedConditions += condition
        op = SelectOp(op, condition)
      }
    }
    
    op
  }
}

sealed abstract class Step

class NodeStep(types: Set[DataType], val generator: Option[AlgebraOp]) extends Step {
  override def toString = "node(" + generator + ")"
}

class LinkStep(val pos: Int, val pattern: RelationalPattern) extends Step {
  override def toString = "link(" + pos + "," + pattern + ")"
}