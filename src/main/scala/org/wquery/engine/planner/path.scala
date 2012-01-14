package org.wquery.engine.planner

import collection.mutable.{ListBuffer, Map}
import org.wquery.engine.operations._
import scalaz._
import Scalaz._
import org.wquery.engine.VariableTemplate

class Path(val links: List[Link], conditions: Map[Option[Link], List[Condition]]) {
  def walkForward(context: BindingsSchema, leftPos: Int, rightPos: Int) = {
    val applier = new ConditionApplier(links, conditions, context)
    val path = links.slice(leftPos, rightPos + 1)

    path.foldLeft(path.head.leftFringe)((op, link) => applier.applyConditions(link.forward(op), link))
  }

  def walkBackward(context: BindingsSchema, leftPos: Int, rightPos: Int) = {
    val applier = new ConditionApplier(links, conditions, context)
    val path = links.slice(leftPos, rightPos + 1)

    path.foldRight(path.last.rightFringe)((link, op) => applier.applyConditions(link.backward(op), link))
  }
}

class PathBuilder {
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

  def build = {
    flushLink

    new Path(linkBuffer.toList, conditions)
  }

}

