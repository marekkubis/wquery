package org.wquery.engine

import scalaz._
import Scalaz._
import org.wquery.{WQueryStepVariableCannotBeBoundException, WQueryEvaluationException}

case class VariableTemplate(pattern: List[Variable]) {
  val variables = pattern.filterNot(_.isUnnamed).toSet

  val pathVariablePosition = {
    val pos = pattern.indexWhere{_.isInstanceOf[PathVariable]}

    if (pos /== pattern.lastIndexWhere{_.isInstanceOf[PathVariable]})
      throw new WQueryEvaluationException("Variable list " + pattern.mkString + " contains more than one path variable")
    else if (pos /== -1)
      some(pos)
    else
      none
  }

  val pathVariableName = pathVariablePosition.map(pattern(_)).filterNot(_.isUnnamed).map(_.name)

  val stepVariableNames = {
    val nameList = pattern.filterNot(x => (x.isInstanceOf[PathVariable] || x.isUnnamed)).map(_.name)
    val distinctNames = nameList.distinct

    if (nameList /== distinctNames)
      throw new WQueryEvaluationException("Variable list " + pattern.mkString + " contains duplicated variable names")
    else
      distinctNames.toSet
  }

  val (leftVariablesIndexes, rightVariablesIndexes) = pathVariablePosition match {
    case Some(pathVarPos) =>
      (pattern.slice(0, pathVarPos).zipWithIndex.filterNot{ case (v, i) => v.isUnnamed }.map{ case (v, i) => (v.name, i) }.toMap,
        pattern.slice(pathVarPos + 1, pattern.size).reverse.zipWithIndex.filterNot{ case (v, i) => v.isUnnamed }.map{ case (v, i) => (v.name, i) }.toMap)
    case None =>
      (Map[String, Int](), pattern.reverse.zipWithIndex.filterNot{ case (v, i) => v.isUnnamed }.map{ case (v, i) => (v.name, i) }.toMap)
  }

  val leftVariablesNames = leftVariablesIndexes.keySet
  val rightVariablesNames = rightVariablesIndexes.keySet
  val leftPatternSize = pathVariablePosition.getOrElse(0)
  val rightPatternSize = pathVariablePosition.some(pos => pattern.size - (pos + 1)).none(pattern.size)

  def leftIndex(variable: String, tupleSize: Int, shift: Int) = {
    val pos = leftVariablesIndexes(variable)

    if (pos < tupleSize)
      shift + pos
    else
      throw new WQueryStepVariableCannotBeBoundException(variable)
  }

  def rightIndex(variable: String, tupleSize: Int, shift: Int) = {
    val pos = tupleSize - 1 - rightVariablesIndexes(variable)

    if (pos >= 0)
      shift + pos
    else
      throw new WQueryStepVariableCannotBeBoundException(variable)
  }

  def pathVariableIndexes(tupleSize: Int, shift: Int) = (shift + leftPatternSize, shift + tupleSize - rightPatternSize)

  override def toString = if (pattern.isEmpty) "novars" else pattern.mkString
}

object VariableTemplate {
  val empty = new VariableTemplate(Nil)
  
  implicit val VariableTemplateZero = zero(empty)
  
  implicit val VariableTemplateEqual = equalA[VariableTemplate]
}