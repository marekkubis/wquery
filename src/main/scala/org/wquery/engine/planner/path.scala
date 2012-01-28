package org.wquery.engine.planner

import collection.mutable.{ListBuffer, Map}
import org.wquery.engine.operations._
import scalaz._
import Scalaz._
import org.wquery.engine.VariableTemplate
import org.wquery.model.{FloatType, IntegerType}

class Path(val links: List[Link], val conditions: Map[Option[Link], List[Condition]])

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

      linkBuffer.append(new PatternLink(linkPos.get, linkPattern.get, variables, inferGenerators(linkPattern.get, conditionsList, variables)))
      linkPos = none
      linkPattern = none
    }

    conditions(linkBuffer.last.some) = conditionsList
    linkVariables = none
    linkConditions = new ListBuffer[Condition]
  }

  private def inferGenerators(linkPattern: RelationalPattern, linkConditions: List[Condition], linkVariables: VariableTemplate) = {
    val buffer = new ListBuffer[(AlgebraOp, Condition)]

    for (condition <- linkConditions) {
      condition matchOrZero {
        case BinaryCondition(op, leftOp, rightOp) if op == "in" || op == "=" =>
            inferGeneratorFromContextVariable(leftOp, rightOp)
              .orElse(inferGeneratorFromContextVariable(rightOp, leftOp))
              .map(generator => buffer.append((adjustGeneratorTypeToPattern(linkPattern, generator), condition)))
      }
    }

    for (condition <- conditions(none)) {
      condition matchOrZero {
        case BinaryCondition(op, leftOp, rightOp) if op == "in" || op == "=" =>
          inferGeneratorFromStepVariable(linkVariables, leftOp, rightOp)
            .orElse(inferGeneratorFromStepVariable(linkVariables, rightOp, leftOp))
            .map(generator => buffer.append((adjustGeneratorTypeToPattern(linkPattern, generator), condition)))
      }
    }

    buffer.toList
  }

  private def inferGeneratorFromContextVariable(contextOp: AlgebraOp, generatorOp: AlgebraOp) = {
    (contextOp matchOrZero {
      case ContextRefOp(_) =>
        some(generatorOp)
    }).filterNot(_.containsReferences)
  }

  private def inferGeneratorFromStepVariable(linkVariables: VariableTemplate, contextOp: AlgebraOp, generatorOp: AlgebraOp) = {
    (contextOp matchOrZero {
      case StepVariableRefOp(variable, _) =>
        (linkVariables.pattern.lastOption.some(_ === variable).none(false))??(some(generatorOp))
    }).filterNot(_.containsReferences)
  }

  private def adjustGeneratorTypeToPattern(linkPattern: RelationalPattern, generatorOp: AlgebraOp) = {
    if (linkPattern.rightType(0) == Set(IntegerType) && generatorOp.rightType(0).contains(FloatType))
      FunctionOp(IntFunction, generatorOp)
    else if (linkPattern.rightType(0) == Set(FloatType) && generatorOp.rightType(0).contains(IntegerType))
      FunctionOp(IntFunction, generatorOp)
    else
      generatorOp
  }

  def build = {
    flushLink

    new Path(linkBuffer.toList, conditions)
  }

}

