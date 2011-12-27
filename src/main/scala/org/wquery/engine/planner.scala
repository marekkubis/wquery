package org.wquery.engine

import collection.mutable.{ListBuffer, Map}
import org.wquery.model.DataType

class LogicalPlanBuilder {
  val steps = new ListBuffer[Step]
  val bindings = Map[Step, VariableTemplate]()
  val conditions = new ListBuffer[Condition]
  
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

    // build condition trigger
    
    conditions.append(condition)
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

    for (step <- path.tail) {
      val stepBindings = bindings.get(step)

      step match {
        case LinkStep(pos, pattern) =>
          op = ExtendOp(op, pos, pattern, stepBindings.map(_.variables).getOrElse(Nil))
        case _ =>
          // do nothing
      }

      stepBindings.map(binds => op = applier.applyConditions(op, binds))
    }

    op
  }
}

class ConditionApplier(conditions: List[Condition]) {
  val appliedConditions = scala.collection.mutable.Set[Condition]()
  val alreadyBoundVariables = scala.collection.mutable.Set[Variable]()

  def applyConditions(inputOp: AlgebraOp, template: VariableTemplate) = {
    var op = inputOp

    for (condition <- conditions.filterNot(appliedConditions.contains(_))) {
      if (condition.referencedVariables.forall { variable =>
        alreadyBoundVariables.contains(variable) || template.variables.contains(variable)
      }) {
        appliedConditions += condition
        alreadyBoundVariables ++= condition.referencedVariables
        op = SelectOp(op, condition)
      }
    }
    
    op
  }
}

sealed abstract class Step

case class NodeStep(types: Set[DataType], generator: Option[AlgebraOp]) extends Step

case class LinkStep(pos: Int, pattern: RelationalPattern) extends Step