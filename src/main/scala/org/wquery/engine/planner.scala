package org.wquery.engine

import collection.mutable.{ListBuffer, Map}
import scalaz._
import Scalaz._

class LogicalPlanBuilder(context: BindingsSchema) {
  val steps = new ListBuffer[Link]
  val bindings = Map[Int, VariableTemplate]()
  val conditions = Map[Option[Int], ListBuffer[Condition]]()
  var currentStep = 0
  var rootGenerator = none[AlgebraOp]

  def createStep(generator: AlgebraOp) {
    rootGenerator = generator.some
  }

  def appendStep(pos: Int, pattern: RelationalPattern) {
    val leftGenerator = (pos == 0).??(rootGenerator)
      
    steps.append(new Link(leftGenerator, pos, pattern, none))
    currentStep += 1
  }

  def appendCondition(condition: Condition) {
    val step = condition.referencesContext.??(currentStep.some)

    if (!conditions.contains(step))
      conditions(step) = new ListBuffer[Condition]

    conditions(step).append(condition)
  }

  def appendVariables(variables: VariableTemplate) {
    bindings(currentStep) = variables
  }

  def build = { // TODO return multiple plans - PlanEvaluator will choose one
    // traverse the graph
    if (steps.isEmpty) {
      ~rootGenerator.map{ generator =>
        val stepOp: AlgebraOp = bindings.get(0).map(template => BindOp(generator, template)).getOrElse(generator)
        val applier = new ConditionApplier(bindings, conditions, context)

        List(applier.applyConditions(stepOp, 0))
      }
    } else {
      // create links
    
      List(
        walkForward(0, steps.size - 1)//, walkBackward(0, steps.size - 1)
      )
    }
  }

  def walkForward(leftPos: Int, rightPos: Int) = {
    val path = steps.slice(leftPos, rightPos + 1).toList
    val applier = new ConditionApplier(bindings, conditions, context)

    var op: AlgebraOp = path.head.leftGenerator.get // TODO if there is no generator use path.head.generate
    op = bindings.get(0).map(template => BindOp(op, template)).getOrElse(op)
    op = applier.applyConditions(op, 0)

    for (step <- 1 to path.size) {
      val link = path(step - 1)

      op = ExtendOp(op, link.pos, link.pattern, Forward, ~bindings.get(step))
      op = applier.applyConditions(op, step)
    }

    op
  }
}

class ConditionApplier(bindings: Map[Int, VariableTemplate], conditions: Map[Option[Int], ListBuffer[Condition]], context: BindingsSchema) {
  val appliedConditions = scala.collection.mutable.Set[Condition]()
  val pathVariables = bindings.values.map(_.variables).foldLeft(Set.empty[Variable])((l, r) => l union r)
  val alreadyBoundVariables = scala.collection.mutable.Set[Variable]()

  def applyConditions(inputOp: AlgebraOp, currentStep: Int) = {
    var op = inputOp
    val template = ~bindings.get(currentStep)

    alreadyBoundVariables ++= template.variables
    val candidateConditions = (~conditions.get(none) ++ ~conditions.get(some(currentStep))).filterNot(appliedConditions.contains)

    for (condition <- candidateConditions) {
      if (condition.referencedVariables.forall { variable =>
        alreadyBoundVariables.contains(variable) || template.variables.contains(variable) || (context.isBound(variable) && !pathVariables.contains(variable))
      }) {
        appliedConditions += condition
        op = SelectOp(op, condition)
      }
    }
    
    op
  }
}

class Link(val leftGenerator: Option[AlgebraOp], val pos: Int, val pattern: RelationalPattern, val rightGenerator: Option[AlgebraOp]) {
  override def toString = "(" + List(leftGenerator, pos, pattern, rightGenerator).mkString(",") + ")"
}