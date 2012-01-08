package org.wquery.engine

import collection.mutable.{ListBuffer, Map}
import scalaz._
import Scalaz._

class LogicalPlanBuilder(context: BindingsSchema) {
  val steps = new ListBuffer[Step]
  val bindings = Map[Step, VariableTemplate]()
  val conditions = new ListBuffer[(Step, Condition)]
  
  def createStep(generator: AlgebraOp) {
    steps.append(new FirstStep(generator))
  }

  def appendStep(pos: Int, pattern: RelationalPattern) {
    steps.append(new NextStep(pos, pattern))
  }

  def appendCondition(condition: Condition) {
    conditions.append((steps.last, condition))
  }

  def appendVariables(variables: VariableTemplate) {
    bindings(steps.last) = variables
  }

  def build = { // TODO return multiple plans - PlanEvaluator will choose one
    // traverse the graph
    List(
      walkForward(0, steps.size - 1)//, walkBackward(0, steps.size - 1)
    )
  }

  def walkForward(leftPos: Int, rightPos: Int) = {
    val path = steps.slice(leftPos, rightPos + 1).toList
    val applier = new ConditionApplier(bindings, conditions.toList, context)

    var op: AlgebraOp = path.head.asInstanceOf[FirstStep].generator // TODO remove this cast
    op = bindings.get(path.head).map(template => BindOp(op, template)).getOrElse(op)
    op = applier.applyConditions(op, path.head)

    for (step <- path.tail) {
      step match {
        case link: NextStep =>
          op = ExtendOp(op, link.pos, link.pattern, Forward, bindings.get(step).orZero)
        case _ =>
          // do nothing
      }

      op = applier.applyConditions(op, step)
    }

    op
  }
}

class ConditionApplier(bindings: Map[Step, VariableTemplate], conditions: List[(Step, Condition)], context: BindingsSchema) {
  val appliedConditions = scala.collection.mutable.Set[Condition]()
  val pathVariables = bindings.values.map(_.variables).foldLeft(Set.empty[Variable])((l, r) => l union r)
  val alreadyBoundVariables = scala.collection.mutable.Set[Variable]()

  def applyConditions(inputOp: AlgebraOp, currentStep: Step) = {
    var op = inputOp
    val template = ~bindings.get(currentStep)

    alreadyBoundVariables ++= template.variables

    for ((step, condition) <- conditions.filterNot{ case (_, c) => appliedConditions.contains(c)}) {
      if (condition.referencedVariables.forall { variable =>
        alreadyBoundVariables.contains(variable) || template.variables.contains(variable) || (context.isBound(variable) && !pathVariables.contains(variable))
      } && (!condition.referencesContext || step == currentStep)) {
        appliedConditions += condition
        op = SelectOp(op, condition)
      }
    }
    
    op
  }
}

sealed abstract class Step

class FirstStep(val generator: AlgebraOp) extends Step {
  override def toString = "fs(" + generator + ")"
}

class NextStep(val pos: Int, val pattern: RelationalPattern) extends Step {
  override def toString = "ns(" + pos + "," + pattern + ")"
}