package org.wquery.engine

import collection.mutable.{ListBuffer, Map}
import scalaz._
import Scalaz._

class LogicalPlanBuilder(context: BindingsSchema) {
  val linkBuffer = new ListBuffer[Link]
  val conditions = Map[Option[Link], List[Condition]]()
  var rootGenerator = none[AlgebraOp]
  var rootLink = true
  var linkVariables = none[VariableTemplate]
  var linkPos = none[Int]
  var linkPattern = none[RelationalPattern]
  var linkConditions = new ListBuffer[Condition]

  conditions(none) = Nil

  def createRootLink(generator: AlgebraOp) {
    rootGenerator = generator.some
  }

  def appendLink(pos: Int, pattern: RelationalPattern) {
    flushLink
    linkPos = pos.some
    linkPattern = pattern.some
  }

  def appendCondition(condition: Condition) {
    if (condition.referencesContext) 
      linkConditions.append(condition)
    else
      conditions(none) = condition::conditions(none)
  }

  def appendVariables(variables: VariableTemplate) {
    linkVariables = variables.some
  }
  
  private def flushLink {
    if (rootLink) {
      val generator = linkVariables.map(variables => BindOp(rootGenerator.get, variables)).getOrElse(rootGenerator.get)

      linkBuffer.append(new RootLink(generator, ~linkVariables))
      rootLink = false
    } else {
      linkBuffer.append(new PatternLink(linkPos.get, linkPattern.get, ~linkVariables))
      linkPos = none
      linkPattern = none
    }

    conditions(linkBuffer.last.some) = linkConditions.toList
    linkVariables = none
    linkConditions = new ListBuffer[Condition]
  }

  def build = { // TODO return multiple plans - PlanEvaluator will choose one
    flushLink

    val links = linkBuffer.toList

    List(
      walkForward(links, 0, links.size - 1)//, walkBackward(0, steps.size)
    )
  }

  def walkForward(links: List[Link], leftPos: Int, rightPos: Int) = {
    val applier = new ConditionApplier(links, conditions, context)
    val path = links.slice(leftPos, rightPos + 1)

    path.foldLeft(path.head.leftFringe)((op, link) => applier.applyConditions(link.forward(op), link))
  }
}

class ConditionApplier(links: List[Link], conditions: Map[Option[Link], List[Condition]], context: BindingsSchema) {
  val appliedConditions = scala.collection.mutable.Set[Condition]()
  val pathVariables = links.map(_.variables.variables).asMA.sum
  val alreadyBoundVariables = scala.collection.mutable.Set[Variable]()

  def applyConditions(inputOp: AlgebraOp, currentLink: Link) = {
    var op = inputOp

    alreadyBoundVariables ++= currentLink.variables.variables
    val candidateConditions = (~conditions.get(none) ++ ~conditions.get(some(currentLink))).filterNot(appliedConditions.contains)

    for (condition <- candidateConditions) {
      if (condition.referencedVariables.forall { variable =>
        alreadyBoundVariables.contains(variable) || currentLink.variables.variables.contains(variable) || (context.isBound(variable) && !pathVariables.contains(variable))
      }) {
        appliedConditions += condition
        op = SelectOp(op, condition)
      }
    }
    
    op
  }
}

sealed abstract class Link(val variables: VariableTemplate) {
  def leftFringe: AlgebraOp
  def forward(op: AlgebraOp): AlgebraOp
}

class RootLink(generator: AlgebraOp, override val variables: VariableTemplate) extends Link(variables) {
  def leftFringe = ConstantOp.empty

  def forward(op: AlgebraOp) = generator

  override def toString = "r(" + generator + ")"
}

// class PatternLink(val leftGenerator: Option[AlgebraOp], val pos: Int, val pattern: RelationalPattern, val variables: VariableTemplate, val rightGenerator: Option[AlgebraOp]) extends Link {
class PatternLink(pos: Int, pattern: RelationalPattern, override val variables: VariableTemplate) extends Link(variables) {
  def leftFringe = FringeOp(pattern, Left)

  def forward(op: AlgebraOp) = ExtendOp(op, pos, pattern, Forward,variables)

  override def toString = "p(" + List(pos, pattern, variables).mkString(",") + ")"
}