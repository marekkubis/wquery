package org.wquery.engine

import collection.mutable.{ListBuffer, Map}
import org.wquery.model.DataType

class LogicalPlanBuilder {
  val steps = new ListBuffer[Step]
  val bindings = Map[Step, VariableTemplate]()
  val conditions = new ListBuffer[(Option[Step], Condition)]
  
  def createStep(generator: AlgebraOp) {
    steps.append(new NodeStep(generator.rightType(0), Some(generator)))
  }

  def appendStep(pos: Int, pattern: RelationalPattern) {
    steps.append(new LinkStep(pos, pattern))
    steps.append(new NodeStep(pattern.rightType(0), None)) // TODO use None or Some depending on type
  }

  def appendCondition(condition: Condition) {    
    // if the filter is context dependednt then check if there is a suiatble variable
    //    if yes then substitute
    //    if not then add $__poz variable

    // build condition trigger
    
    conditions.append((steps.lastOption, condition))
  }

  def appendVariables(variables: List[Variable]) {
    // TODO distinguish left and right traverses for step variables
    val step = if (steps.size == 1) steps.last else steps(steps.size - 2)
    bindings(step) = new VariableTemplate(variables)
  }

  def build = { // TODO return multiple plans - PlanEvaluator will choose one
    // travrse the graph biinding variables and firing trigers
    
    walkForward(0, steps.size - 1)
  }

  private def walkForward(leftPos: Int, rightPos: Int) = {
    val path = steps.slice(leftPos, rightPos + 1).toList
    val applier = new ConditionApplier(conditions.toList)

    var op: AlgebraOp = BindOp(path.head.asInstanceOf[NodeStep].generator.get, bindings.get(path.head).map(_.variables).getOrElse(Nil)) // TODO remove this cast and get
    op = applier.applyConditions(op, bindings.get(path.head).getOrElse(VariableTemplate.empty), path.head)

    for (step <- path.tail) {
      val stepBindings = bindings.get(step)

      step match {
        case link: LinkStep =>
          op = ExtendOp(op, link.pos, link.pattern, stepBindings.map(_.variables).getOrElse(Nil))
        case _ =>
          // do nothing
      }

      op = applier.applyConditions(op, stepBindings.getOrElse(VariableTemplate.empty), step)
    }

    op
  }
}

class ConditionApplier(conditions: List[(Option[Step], Condition)]) {
  val appliedConditions = scala.collection.mutable.Set[Condition]()
  val alreadyBoundVariables = scala.collection.mutable.Set[Variable]()

  def applyConditions(inputOp: AlgebraOp, template: VariableTemplate, currentStep: Step) = {
    var op = inputOp
    alreadyBoundVariables ++= template.variables.filterNot(_.name == "_") // TODO move to namedVariables, remove template, use current step

    for ((step, condition) <- conditions.filterNot{ case (_, c) => appliedConditions.contains(c)}) {
      if (condition.referencedVariables.forall { variable =>
        alreadyBoundVariables.contains(variable) || template.variables.contains(variable)
      } && (!condition.referencesContext || step.map(_ == currentStep).getOrElse(false))) {
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