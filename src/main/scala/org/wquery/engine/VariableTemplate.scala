package org.wquery.engine
import org.wquery.WQueryEvaluationException

case class VariableTemplate(variables: List[Variable]) {
  val pathVariablePosition = {
    val pos = variables.indexWhere{_.isInstanceOf[PathVariable]}

    if (pos != variables.lastIndexWhere{_.isInstanceOf[PathVariable]})
      throw new WQueryEvaluationException("Variable list " + variables.mkString + " contains more than one path variable")
    else if (pos != -1)
      Some(pos)
    else
      None
  }

  val pathVariableName = {
    val name = pathVariablePosition.map(variables(_).name)
    if (name.map(_ != "_").getOrElse(false)) name else None
  }

  val stepVariableNames = {
    val nameList = variables.filterNot(x => (x.isInstanceOf[PathVariable] || x.name == "_")).map(_.name)
    val distinctNames = nameList.distinct

    if (nameList != distinctNames)
      throw new WQueryEvaluationException("Variable list " + variables.mkString + " contains duplicated variable names")
    else
      distinctNames.toSet
  }

  val (leftVariablesIndexes, rightVariablesIndexes) = pathVariablePosition match {
    case Some(pathVarPos) =>
      (variables.slice(0, pathVarPos).map(_.name).zipWithIndex.filterNot{_._1 == "_"}.toMap,
        variables.slice(pathVarPos + 1, variables.size).map(_.name).reverse.zipWithIndex.filterNot{_._1 == "_"}.toMap)
    case None =>
      (Map[String, Int](), variables.map(_.name).reverse.zipWithIndex.filterNot{_._1 == "_"}.toMap)
  }

  val leftVariablesNames = leftVariablesIndexes.keySet
  val rightVariablesNames = rightVariablesIndexes.keySet

  def leftIndex(variable: String, tupleSize: Int, shift: Int) = {
    val pos = leftVariablesIndexes(variable)

    if (pos < tupleSize)
      shift + pos
    else
      throw new WQueryEvaluationException("Variable $" + variable + " cannot be bound")
  }

  def rightIndex(variable: String, tupleSize: Int, shift: Int) = {
    val pos = tupleSize - 1 - rightVariablesIndexes(variable)

    if (pos >= 0)
      shift + pos
    else
      throw new WQueryEvaluationException("Variable $" + variable + " cannot be bound")
  }

  private val leftSize = pathVariablePosition.getOrElse(0)
  private val rightSize = pathVariablePosition.map(pos => variables.size - (pos + 1)).getOrElse(variables.size)

  def pathVariableIndexes(tupleSize: Int, shift: Int) = (shift + leftSize, shift + tupleSize - rightSize)
}

object VariableTemplate {
  val empty = new VariableTemplate(Nil)
}