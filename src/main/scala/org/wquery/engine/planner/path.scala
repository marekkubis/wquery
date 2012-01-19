package org.wquery.engine.planner

import collection.mutable.{ListBuffer, Map}
import org.wquery.engine.operations._
import scalaz._
import Scalaz._
import org.wquery.engine.VariableTemplate
import org.wquery.model.{WQueryOptionNoneMaxOrdering, WordNetSchema}

class Path(val links: List[Link], conditions: Map[Option[Link], List[Condition]]) {
  def walkForward(context: BindingsSchema, leftPos: Int, rightPos: Int) = {
    val applier = new ConditionApplier(links, conditions, context)
    val path = links.slice(leftPos, rightPos + 1)

    path.foldLeft(path.head.leftFringe)((op, link) => applier.applyConditions(link.forward(op), link))
  }

  def walkBackward(wordNet: WordNetSchema, context: BindingsSchema, leftPos: Int, rightPos: Int) = {
    val applier = new ConditionApplier(links, conditions, context)
    val path = links.slice(leftPos, rightPos + 1)
    val (generator, condition) = path.last.rightFringe.minBy(_._1.maxCount(wordNet))(WQueryOptionNoneMaxOrdering.BigIntOrdering)

    condition.map(applier.skipCondition(_))
    path.foldRight(generator)((link, op) => applier.applyConditions(link.backward(op), link))
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
    val conditionsList = linkConditions.toList

    if (rootLink) {
      val generator = linkVariables.map(variables => BindOp(rootGenerator.get, variables)).getOrElse(rootGenerator.get)

      linkBuffer.append(new RootLink(generator, ~linkVariables))
      rootLink = false
    } else {
      val variables = ~linkVariables

      linkBuffer.append(new PatternLink(linkPos.get, linkPattern.get, variables, inferGenerators(conditionsList, variables)))
      linkPos = none
      linkPattern = none
    }

    conditions(linkBuffer.last.some) = conditionsList
    linkVariables = none
    linkConditions = new ListBuffer[Condition]
  }

  private def inferGenerators(linkConditions: List[Condition], linkVariables: VariableTemplate) = {
    val buffer = new ListBuffer[(AlgebraOp, Condition)]

    for (condition <- linkConditions) {
      condition matchOrZero {
        case BinaryCondition(op, leftOp, rightOp) if op == "in" || op == "=" =>
            inferGeneratorFromContextVariable(leftOp, rightOp)
              .orElse(inferGeneratorFromContextVariable(rightOp, leftOp))
              .map(generator => buffer.append((generator, condition)))
      }
    }

    for (condition <- conditions(none)) {
      condition matchOrZero {
        case BinaryCondition(op, leftOp, rightOp) if op == "in" || op == "=" =>
          inferGeneratorFromStepVariable(linkVariables, leftOp, rightOp)
            .orElse(inferGeneratorFromStepVariable(linkVariables, rightOp, leftOp))
            .map(generator => buffer.append((generator, condition)))
      }
    }

    buffer.toList
  }

  private def inferGeneratorFromContextVariable(contextOp: AlgebraOp, generatorOp: AlgebraOp) = {
    (contextOp matchOrZero {
      case ContextRefOp(_) =>
        some(generatorOp)
    }).filterNot(_.referencesContext)
  }

  private def inferGeneratorFromStepVariable(linkVariables: VariableTemplate, contextOp: AlgebraOp, generatorOp: AlgebraOp) = {
    (contextOp matchOrZero {
      case StepVariableRefOp(variable, _) =>
        (linkVariables.pattern.lastOption.some(_ === variable).none(false))??(some(generatorOp))
    }).filterNot(_.referencesContext)
  }

  def build = {
    flushLink

    new Path(linkBuffer.toList, conditions)
  }

}

