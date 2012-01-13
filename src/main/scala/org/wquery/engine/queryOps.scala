package org.wquery.engine

import org.wquery.model._
import scalaz._
import Scalaz._
import org.wquery.WQueryEvaluationException

sealed abstract class QueryOp extends AlgebraOp

/*
 * Imperative operations
 */
case class EmitOp(op: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = op.evaluate(wordNet, bindings)

  def leftType(pos: Int) = op.leftType(pos)

  def rightType(pos: Int) = op.rightType(pos)

  def minTupleSize = op.minTupleSize

  def maxTupleSize = op.maxTupleSize

  def bindingsPattern = op.bindingsPattern

  def referencedVariables = op.referencedVariables

  def referencesContext = op.referencesContext
}

case class IterateOp(bindingOp: AlgebraOp, iteratedOp: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val bindingSet = bindingOp.evaluate(wordNet, bindings)
    val buffer = new DataSetBuffer
    val pathVarNames = bindingSet.pathVars.keys.toSeq
    val stepVarNames = bindingSet.stepVars.keys.toSeq

    for (i <- 0 until bindingSet.pathCount) {
      val tuple = bindingSet.paths(i)

      pathVarNames.foreach { pathVar =>
        val varPos = bindingSet.pathVars(pathVar)(i)
        bindings.bindPathVariable(pathVar, tuple.slice(varPos._1, varPos._2))
      }

      stepVarNames.foreach(stepVar => bindings.bindStepVariable(stepVar, tuple(bindingSet.stepVars(stepVar)(i))))
      buffer.append(iteratedOp.evaluate(wordNet, bindings))
    }

    buffer.toDataSet
  }

  def leftType(pos: Int) = iteratedOp.leftType(pos)

  def rightType(pos: Int) = iteratedOp.rightType(pos)

  def minTupleSize = iteratedOp.minTupleSize

  def maxTupleSize = iteratedOp.maxTupleSize

  def bindingsPattern = iteratedOp.bindingsPattern

  def referencedVariables = (iteratedOp.referencedVariables -- bindingOp.bindingsPattern.variables) ++ bindingOp.referencedVariables

  def referencesContext = iteratedOp.referencesContext || bindingOp.referencesContext
}

case class IfElseOp(conditionOp: AlgebraOp, ifOp: AlgebraOp, elseOp: Option[AlgebraOp]) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    if (conditionOp.evaluate(wordNet, bindings).isTrue)
      ifOp.evaluate(wordNet, bindings)
    else
      elseOp.map(_.evaluate(wordNet, bindings)).orZero
  }

  def leftType(pos: Int) = ifOp.leftType(pos) ++ elseOp.map(_.leftType(pos)).orZero

  def rightType(pos: Int) = ifOp.rightType(pos) ++ elseOp.map(_.rightType(pos)).orZero

  def minTupleSize = elseOp.some(_.minTupleSize.min(ifOp.minTupleSize)).none(ifOp.minTupleSize)

  def maxTupleSize = elseOp.map(_.maxTupleSize.map(elseSize => ifOp.maxTupleSize.map(_.max(elseSize))).getOrElse(none)).getOrElse(ifOp.maxTupleSize)

  def bindingsPattern = elseOp.some(_.bindingsPattern union ifOp.bindingsPattern).none(ifOp.bindingsPattern)

  def referencedVariables = conditionOp.referencedVariables ++ ifOp.referencedVariables ++ elseOp.map(_.referencedVariables).orZero

  def referencesContext = conditionOp.referencesContext || ifOp.referencesContext || elseOp.some(_.referencesContext).none(false)
}

case class BlockOp(ops: List[AlgebraOp]) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val blockBindings = Bindings(bindings, true)
    val buffer = new DataSetBuffer

    for (op <- ops)
      buffer.append(op.evaluate(wordNet, blockBindings))

    buffer.toDataSet
  }

  def leftType(pos: Int) = ops.map(_.leftType(pos)).flatten.toSet

  def rightType(pos: Int) = ops.map(_.rightType(pos)).flatten.toSet

  def minTupleSize = ops.map(_.minTupleSize).min

  def maxTupleSize = {
    val opSizes = ops.map(_.maxTupleSize).collect { case Some(x) => x}

    if (opSizes.size != ops.size) none else some(opSizes.max)
  }

  def bindingsPattern = {
    // It is assumed that all statements in a block provide same binding schemas
    ops.headOption.map(_.bindingsPattern).getOrElse(BindingsPattern())
  }

  def referencedVariables = ops.map(_.referencedVariables).asMA.sum

  def referencesContext = ops.exists(_.referencesContext)
}

case class AssignmentOp(variables: VariableTemplate, op: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val result = op.evaluate(wordNet, bindings)

    if (result.pathCount == 1) { // TODO remove this constraint
      val tuple = result.paths.head
      val dataSet = DataSet.fromTuple(tuple).bindVariables(variables)

      dataSet.pathVars.keys.foreach { pathVar =>
        val varPos = dataSet.pathVars(pathVar)(0)
        bindings.bindPathVariable(pathVar, tuple.slice(varPos._1, varPos._2))
      }

      dataSet.stepVars.keys.foreach(stepVar => bindings.bindStepVariable(stepVar, tuple(dataSet.stepVars(stepVar)(0))))
      DataSet.empty
    } else {
      throw new WQueryEvaluationException("A multipath expression in an assignment should contain exactly one tuple")
    }
  }

  def leftType(pos: Int) = Set.empty

  def rightType(pos: Int) = Set.empty

  def minTupleSize = 0

  def maxTupleSize = some(0)

  def bindingsPattern = BindingsPattern()

  def referencedVariables = op.referencedVariables

  def referencesContext = op.referencesContext
}

case class WhileDoOp(conditionOp: AlgebraOp, iteratedOp: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val buffer = new DataSetBuffer

    while (conditionOp.evaluate(wordNet, bindings).isTrue)
      buffer.append(iteratedOp.evaluate(wordNet, bindings))

    buffer.toDataSet
  }

  def leftType(pos: Int) = iteratedOp.leftType(pos)

  def rightType(pos: Int) = iteratedOp.rightType(pos)

  def minTupleSize = iteratedOp.minTupleSize

  def maxTupleSize = iteratedOp.maxTupleSize

  def bindingsPattern = iteratedOp.bindingsPattern

  def referencedVariables = conditionOp.referencedVariables ++ iteratedOp.referencedVariables

  def referencesContext = conditionOp.referencesContext || iteratedOp.referencesContext
}

/*
 * Set operations
 */
case class UnionOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    DataSet(leftOp.evaluate(wordNet, bindings).paths union rightOp.evaluate(wordNet, bindings).paths)
  }

  def leftType(pos: Int) = leftOp.leftType(pos) ++ rightOp.leftType(pos)

  def rightType(pos: Int) = leftOp.rightType(pos) ++ rightOp.rightType(pos)

  def minTupleSize = leftOp.minTupleSize min rightOp.minTupleSize

  def maxTupleSize = (leftOp.maxTupleSize <|*|> rightOp.maxTupleSize).map{ case (a, b) => a max b }

  def bindingsPattern = BindingsPattern()

  def referencedVariables = leftOp.referencedVariables ++ rightOp.referencedVariables

  def referencesContext = leftOp.referencesContext || rightOp.referencesContext
}

case class ExceptOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val leftSet = leftOp.evaluate(wordNet, bindings)
    val rightSet = rightOp.evaluate(wordNet, bindings)

    DataSet(leftSet.paths.filterNot(rightSet.paths.contains))
  }

  def leftType(pos: Int) = leftOp.leftType(pos)

  def rightType(pos: Int) = leftOp.rightType(pos)

  def minTupleSize = leftOp.minTupleSize

  def maxTupleSize = leftOp.maxTupleSize

  def bindingsPattern = BindingsPattern()

  def referencedVariables = leftOp.referencedVariables ++ rightOp.referencedVariables

  def referencesContext = leftOp.referencesContext || rightOp.referencesContext
}

case class IntersectOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    DataSet(leftOp.evaluate(wordNet, bindings).paths intersect rightOp.evaluate(wordNet, bindings).paths)
  }

  def leftType(pos: Int) = leftOp.leftType(pos) intersect rightOp.leftType(pos)

  def rightType(pos: Int) = leftOp.rightType(pos) intersect rightOp.rightType(pos)

  def minTupleSize = leftOp.minTupleSize max rightOp.minTupleSize

  def maxTupleSize = (leftOp.maxTupleSize <|*|> rightOp.maxTupleSize).map{ case (a, b) => a min b}

  def bindingsPattern = BindingsPattern()

  def referencedVariables = leftOp.referencedVariables ++ rightOp.referencedVariables

  def referencesContext = leftOp.referencesContext || rightOp.referencesContext
}

case class JoinOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val leftSet = leftOp.evaluate(wordNet, bindings)
    val rightSet = rightOp.evaluate(wordNet, bindings)
    val leftPathVarNames = leftSet.pathVars.keySet
    val leftStepVarNames = leftSet.stepVars.keySet
    val rightPathVarNames = rightSet.pathVars.keySet
    val rightStepVarNames = rightSet.stepVars.keySet
    val pathBuffer = DataSetBuffers.createPathBuffer
    val pathVarBuffers = DataSetBuffers.createPathVarBuffers(leftPathVarNames union rightPathVarNames)
    val stepVarBuffers = DataSetBuffers.createStepVarBuffers(leftStepVarNames union rightStepVarNames)

    for (i <- 0 until leftSet.pathCount; j <- 0 until rightSet.pathCount) {
      val leftTuple = leftSet.paths(i)
      val rightTuple = rightSet.paths(j)

      pathBuffer.append(leftTuple ++ rightTuple)
      leftPathVarNames.foreach(x => pathVarBuffers(x).append(leftSet.pathVars(x)(i)))
      leftStepVarNames.foreach(x => stepVarBuffers(x).append(leftSet.stepVars(x)(i)))

      val offset = leftTuple.size

      rightPathVarNames.foreach { x =>
        val pos = rightSet.pathVars(x)(j)
        pathVarBuffers(x).append((pos._1 + offset, pos._2 + offset))
      }

      rightStepVarNames.foreach(x => stepVarBuffers(x).append(rightSet.stepVars(x)(j) + offset))
    }

    DataSet.fromBuffers(pathBuffer, pathVarBuffers, stepVarBuffers)
  }

  def leftType(pos: Int) ={
    val leftMinSize = leftOp.minTupleSize
    val leftMaxSize = leftOp.maxTupleSize

    if (pos < leftMinSize) {
      leftOp.leftType(pos)
    } else if (leftMaxSize.map(pos < _).getOrElse(true)) { // pos < leftMaxSize or leftMaxSize undefined
      val rightOpTypes = for (i <- 0 to pos - leftMinSize) yield rightOp.leftType(i)

      (rightOpTypes :+ leftOp.leftType(pos)).flatten.toSet
    } else { // leftMaxSize defined and pos >= leftMaxSize
      (for (i <- leftMinSize to leftMaxSize.get) yield rightOp.leftType(pos - i)).flatten.toSet
    }
  }

  def rightType(pos: Int) ={
    val rightMinSize = rightOp.minTupleSize
    val rightMaxSize = rightOp.maxTupleSize

    if (pos < rightMinSize) {
      rightOp.rightType(pos)
    } else if (rightMaxSize.map(pos < _).getOrElse(true)) { // pos < rightMaxSize or rightMaxSize undefined
      val leftOpTypes = for (i <- 0 to pos - rightMinSize) yield leftOp.rightType(i)

      (leftOpTypes :+ rightOp.rightType(pos)).flatten.toSet
    } else { // rightMaxSize defined and pos >= rightMaxSize
      (for (i <- rightMinSize to rightMaxSize.get) yield leftOp.rightType(pos - i)).flatten.toSet
    }
  }

  def minTupleSize = leftOp.minTupleSize + rightOp.minTupleSize

  def maxTupleSize = (leftOp.maxTupleSize <|*|> rightOp.maxTupleSize).map{ case (a, b) => a + b }

  def bindingsPattern = leftOp.bindingsPattern union rightOp.bindingsPattern

  def referencedVariables = leftOp.referencedVariables ++ rightOp.referencedVariables

  def referencesContext = leftOp.referencesContext || rightOp.referencesContext
}

/*
 * Arithmetic operations
 */
abstract class BinaryArithmeticOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val leftSet = leftOp.evaluate(wordNet, bindings).paths.map(_.last)
    val rightSet = rightOp.evaluate(wordNet, bindings).paths.map(_.last)

    DataSet(for (leftVal <- leftSet; rightVal <- rightSet) yield List(combine(leftVal, rightVal)))
  }

  def combine(left: Any, right: Any): Any

  def leftType(pos: Int) = if (pos == 0) {
    val leftOpType = leftOp.leftType(pos)
    val rightOpType = rightOp.leftType(pos)

    if (leftOpType == rightOpType) leftOpType else Set(FloatType)
  } else {
    Set.empty
  }

  def rightType(pos: Int) = if (pos == 0) {
    val leftOpType = leftOp.rightType(pos)
    val rightOpType = rightOp.rightType(pos)

    if (leftOpType == rightOpType) leftOpType else Set(FloatType)
  } else {
    Set.empty
  }

  def minTupleSize = 1

  def maxTupleSize = some(1)

  def bindingsPattern = BindingsPattern()

  def referencedVariables = leftOp.referencedVariables ++ rightOp.referencedVariables

  def referencesContext = leftOp.referencesContext || rightOp.referencesContext
}

case class AddOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends BinaryArithmeticOp(leftOp, rightOp) {
  def combine(leftVal: Any, rightVal: Any): Any = (leftVal, rightVal) match {
    case (leftVal: Double, rightVal: Double) =>
      leftVal + rightVal
    case (leftVal: Double, rightVal: Int) =>
      leftVal + rightVal
    case (leftVal: Int, rightVal: Double) =>
      leftVal + rightVal
    case (leftVal: Int, rightVal: Int) =>
      leftVal + rightVal
  }
}

case class SubOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends BinaryArithmeticOp(leftOp, rightOp) {
  def combine(leftVal: Any, rightVal: Any): Any = (leftVal, rightVal) match {
    case (leftVal: Double, rightVal: Double) =>
      leftVal - rightVal
    case (leftVal: Double, rightVal: Int) =>
      leftVal - rightVal
    case (leftVal: Int, rightVal: Double) =>
      leftVal - rightVal
    case (leftVal: Int, rightVal: Int) =>
      leftVal - rightVal
  }
}

case class MulOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends BinaryArithmeticOp(leftOp, rightOp) {
  def combine(leftVal: Any, rightVal: Any): Any = (leftVal, rightVal) match {
    case (leftVal: Double, rightVal: Double) =>
      leftVal * rightVal
    case (leftVal: Double, rightVal: Int) =>
      leftVal * rightVal
    case (leftVal: Int, rightVal: Double) =>
      leftVal * rightVal
    case (leftVal: Int, rightVal: Int) =>
      leftVal * rightVal
  }
}

case class DivOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends BinaryArithmeticOp(leftOp, rightOp) {
  def combine(leftVal: Any, rightVal: Any): Any = (leftVal, rightVal) match {
    case (leftVal: Double, rightVal: Double) =>
      leftVal / rightVal
    case (leftVal: Double, rightVal: Int) =>
      leftVal / rightVal
    case (leftVal: Int, rightVal: Double) =>
      leftVal / rightVal
    case (leftVal: Int, rightVal: Int) =>
      leftVal.toDouble / rightVal.toDouble
  }
}

case class IntDivOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends BinaryArithmeticOp(leftOp, rightOp) {
  def combine(leftVal: Any, rightVal: Any) = (leftVal, rightVal) match {
    case (leftVal: Int, rightVal: Int) =>
      leftVal / rightVal
  }
}

case class ModOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends BinaryArithmeticOp(leftOp, rightOp) {
  def combine(leftVal: Any, rightVal: Any): Any = (leftVal, rightVal) match {
    case (leftVal: Double, rightVal: Double) =>
      leftVal % rightVal
    case (leftVal: Double, rightVal: Int) =>
      leftVal % rightVal
    case (leftVal: Int, rightVal: Double) =>
      leftVal % rightVal
    case (leftVal: Int, rightVal: Int) =>
      leftVal % rightVal
  }
}

case class MinusOp(op: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    DataSet(op.evaluate(wordNet, bindings).paths.map(_.last).map {
        case x: Int => List(-x)
        case x: Double => List(-x)
    })
  }

  def leftType(pos: Int) = op.leftType(pos)

  def rightType(pos: Int) = op.rightType(pos)

  def minTupleSize = 1

  def maxTupleSize = some(1)

  def bindingsPattern = BindingsPattern()

  def referencedVariables = op.referencedVariables

  def referencesContext = op.referencesContext
}

case class FunctionOp(function: Function, args: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = function.evaluate(args, wordNet, bindings)

  def leftType(pos: Int) = function.leftType(args, pos)

  def rightType(pos: Int) = function.rightType(args, pos)

  def minTupleSize = function.minTupleSize(args)

  def maxTupleSize = function.maxTupleSize(args)

  def bindingsPattern = function.bindingsPattern(args)

  def referencedVariables = args.referencedVariables

  def referencesContext = args.referencesContext
}

/*
 * Declarative operations
 */
case class SelectOp(op: AlgebraOp, condition: Condition) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val dataSet = op.evaluate(wordNet, bindings)
    val pathVarNames = dataSet.pathVars.keySet
    val stepVarNames = dataSet.stepVars.keySet
    val pathBuffer = DataSetBuffers.createPathBuffer
    val pathVarBuffers = DataSetBuffers.createPathVarBuffers(pathVarNames)
    val stepVarBuffers = DataSetBuffers.createStepVarBuffers(stepVarNames)
    val binds = Bindings(bindings, false)

    // TODO OPT here determine which variables are to be used by filter

    for (i <- 0 until dataSet.pathCount) {
      val tuple = dataSet.paths(i)

      for (pathVar <- pathVarNames) {
        val varPos = dataSet.pathVars(pathVar)(i)
        binds.bindPathVariable(pathVar, tuple.slice(varPos._1, varPos._2))
      }

      for (stepVar <- stepVarNames) {
        val varPos = dataSet.stepVars(stepVar)(i)
        binds.bindStepVariable(stepVar, tuple(varPos))
      }

      binds.bindContextVariable(tuple.last)

      if (condition.satisfied(wordNet, binds)) {
        pathBuffer.append(tuple)
        pathVarNames.foreach(x => pathVarBuffers(x).append(dataSet.pathVars(x)(i)))
        stepVarNames.foreach(x => stepVarBuffers(x).append(dataSet.stepVars(x)(i)))
      }
    }

    DataSet.fromBuffers(pathBuffer, pathVarBuffers, stepVarBuffers)
  }

  def leftType(pos: Int) = op.leftType(pos)

  def rightType(pos: Int) = op.rightType(pos)

  def minTupleSize = op.minTupleSize

  def maxTupleSize = op.maxTupleSize

  def bindingsPattern = op.bindingsPattern

  def referencedVariables = op.referencedVariables ++ (condition.referencedVariables -- op.bindingsPattern.variables)

  def referencesContext = op.referencesContext
}

case class ProjectOp(op: AlgebraOp, projectOp: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val dataSet = op.evaluate(wordNet, bindings)
    val buffer = new DataSetBuffer
    val pathVarNames = dataSet.pathVars.keys
    val stepVarNames = dataSet.stepVars.keys
    val binds = Bindings(bindings, false)

    // TODO OPT here determine which variables are to be used by projection

    for (i <- 0 until dataSet.pathCount) {
      val tuple = dataSet.paths(i)

      binds.bindContextVariable(tuple.last)

      for (pathVar <- pathVarNames) {
        val varPos = dataSet.pathVars(pathVar)(i)
        binds.bindPathVariable(pathVar, tuple.slice(varPos._1, varPos._2))
      }

      for (stepVar <- stepVarNames) {
        val varPos = dataSet.stepVars(stepVar)(i)
        binds.bindStepVariable(stepVar, tuple(varPos))
      }

      buffer.append(projectOp.evaluate(wordNet, binds))
    }

    buffer.toDataSet
  }

  def leftType(pos: Int) = projectOp.leftType(pos)

  def rightType(pos: Int) = projectOp.rightType(pos)

  def minTupleSize = projectOp.minTupleSize

  def maxTupleSize = projectOp.maxTupleSize

  def bindingsPattern = projectOp.bindingsPattern

  def referencedVariables = op.referencedVariables ++ (projectOp.referencedVariables -- op.bindingsPattern.variables)

  def referencesContext = op.referencesContext
}

case class ExtendOp(op: AlgebraOp, from: Int, pattern: RelationalPattern, direction: Direction, variables: VariableTemplate) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val dataSet = op.evaluate(wordNet, bindings)
    val extensionSet = pattern.extend(wordNet.store, bindings, new DataExtensionSet(dataSet), from, direction)
    val dataSetPathVarNames = dataSet.pathVars.keySet
    val dataSetStepVarNames = dataSet.stepVars.keySet
    val pathBuffer = DataSetBuffers.createPathBuffer
    val pathVarBuffers = DataSetBuffers.createPathVarBuffers(variables.pathVariableName.map(dataSetPathVarNames + _).getOrElse(dataSetPathVarNames))
    val stepVarBuffers = DataSetBuffers.createStepVarBuffers(dataSetStepVarNames union variables.stepVariableNames)

    for ((pathPos, extension) <- extensionSet.extensions) {
      val path = dataSet.paths(pathPos)
      val extensionSize = extension.size

      val (pathShift, extensionShift) = direction match {
        case Forward =>
          pathBuffer.append(path ++ extension)
          (0, path.size)
        case Backward =>
          pathBuffer.append(extension ++ path)
          (extensionSize, 0)
      }

      for (pathVar <- dataSetPathVarNames) {
        val (left, right) = dataSet.pathVars(pathVar)(pathPos)
        pathVarBuffers(pathVar).append((left + pathShift, right + pathShift))
      }

      for (stepVar <- dataSetStepVarNames)
        stepVarBuffers(stepVar).append(dataSet.stepVars(stepVar)(pathPos) + pathShift)

      for (pathVar <- variables.pathVariableName)
        pathVarBuffers(pathVar).append(variables.pathVariableIndexes(extensionSize, extensionShift))

      for (stepVar <- variables.leftVariablesNames)
        stepVarBuffers(stepVar).append(variables.leftIndex(stepVar, extensionSize, extensionShift))

      for (stepVar <- variables.rightVariablesNames)
        stepVarBuffers(stepVar).append(variables.rightIndex(stepVar, extensionSize, extensionShift))
    }

    DataSet.fromBuffers(pathBuffer, pathVarBuffers, stepVarBuffers)
  }

  def leftType(pos: Int) = {
    if (pos < op.minTupleSize) {
      op.leftType(pos)
    } else if (op.maxTupleSize.map(pos < _).getOrElse(true)) { // pos < maxTupleSize or maxTupleSize undefined
      val extendOpTypes = for (i <- 0 to pos - op.minTupleSize) yield pattern.leftType(i)

      (extendOpTypes :+ op.leftType(pos)).flatten.toSet
    } else { // maxTupleSize defined and pos >= maxTupleSize
      (for (i <- op.minTupleSize to op.maxTupleSize.get) yield pattern.leftType(pos - i)).flatten.toSet
    }
  }

  def rightType(pos: Int) = {
    if (pos < pattern.minSize) {
      pattern.rightType(pos)
    } else if (pattern.maxSize.map(pos < _).getOrElse(true)) { // pos < maxSize or maxSize undefined
      val extendOpTypes = for (i <- 0 to pos - pattern.minSize) yield op.rightType(i)

      (extendOpTypes :+ pattern.rightType(pos)).flatten.toSet
    } else { // maxSize defined and pos >= maxSize
      (for (i <- pattern.minSize to pattern.maxSize.get) yield op.rightType(pos - i)).flatten.toSet
    }
  }

  def minTupleSize = op.minTupleSize + pattern.minSize

  def maxTupleSize = (op.maxTupleSize <|*|> pattern.maxSize).map{ case (a, b) => a + b }

  def bindingsPattern = {
    val pattern = op.bindingsPattern
    pattern.bindVariablesTypes(variables, this)
    pattern
  }

  def referencedVariables = op.referencedVariables

  def referencesContext = op.referencesContext
}

case class Quantifier(lowerBound: Int, upperBound: Option[Int]) {
  override def toString = "{" + lowerBound + upperBound.map(ub => if (lowerBound == ub) "}" else "," + ub  + "}").getOrElse(",}")
}

case class ArcPatternArgument(name: String, nodeType: Option[NodeType]) {
  override def toString = name + ~nodeType.map("&" + _)
}

case class BindOp(op: AlgebraOp, variables: VariableTemplate) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    op.evaluate(wordNet, bindings).bindVariables(variables)
  }

  def leftType(pos: Int) = op.leftType(pos)

  def rightType(pos: Int) = op.rightType(pos)

  def minTupleSize = op.minTupleSize

  def maxTupleSize = op.maxTupleSize

  def bindingsPattern = {
    val pattern = op.bindingsPattern
    pattern.bindVariablesTypes(variables, op)
    pattern
  }

  def referencedVariables = op.referencedVariables

  def referencesContext = op.referencesContext
}

case class FringeOp(pattern: RelationalPattern, side: Side) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = pattern.fringe(wordNet.store, bindings, side)

  def leftType(pos: Int) = (pos == 0).??(pattern.leftType(0))

  def rightType(pos: Int) = (pos == 0).??(pattern.rightType(0))

  def minTupleSize = 1

  def maxTupleSize = some(1)

  def bindingsPattern = BindingsPattern()

  def referencedVariables = Set.empty

  def referencesContext = false
}

/*
 * Elementary operations
 */
case class FetchOp(relation: Relation, from: List[(String, List[Any])], to: List[String]) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = wordNet.store.fetch(relation, from, to)

  def leftType(pos: Int) = typeAt(pos)

  def rightType(pos: Int) = typeAt(from.size + to.size - 1 - pos)

  private def typeAt(pos: Int): Set[DataType] = {
    val args = from.map(_._1) ++ to

    if (args.isDefinedAt(pos))
      Set(relation.demandArgument(args(pos)).nodeType)
    else
      Set.empty
  }

  def minTupleSize = to.size

  def maxTupleSize = some(to.size)

  def bindingsPattern = BindingsPattern()

  def referencedVariables = Set.empty

  def referencesContext = false
}

object FetchOp {
  def words = FetchOp(WordNet.WordSet, List((Relation.Source, Nil)), List(Relation.Source))

  def senses = FetchOp(WordNet.SenseSet, List((Relation.Source, Nil)), List(Relation.Source))

  def synsets = FetchOp(WordNet.SynsetSet, List((Relation.Source, Nil)), List(Relation.Source))

  def possyms = FetchOp(WordNet.PosSet, List((Relation.Source, Nil)), List(Relation.Source))

  def wordByValue(value: String)
    = FetchOp(WordNet.WordSet, List((Relation.Source, List(value))), List(Relation.Source))

  def senseByWordFormAndSenseNumberAndPos(word: String, num: Int, pos: String) = {
    FetchOp(WordNet.SenseToWordFormSenseNumberAndPos,
      List((Relation.Destination, List(word)), ("num", List(num)), ("pos", List(pos))), List(Relation.Source))
  }

  def sensesByWordFormAndSenseNumber(word: String, num: Int) = {
    FetchOp(WordNet.SenseToWordFormSenseNumberAndPos,
      List((Relation.Destination, List(word)), ("num", List(num))), List(Relation.Source))
  }
}

case class ConstantOp(dataSet: DataSet) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = dataSet

  def leftType(pos: Int) = dataSet.leftType(pos)

  def rightType(pos: Int) = dataSet.rightType(pos)

  def minTupleSize = dataSet.minTupleSize

  def maxTupleSize = some(dataSet.maxTupleSize)

  def bindingsPattern = BindingsPattern() // assumed that constant dataset does not contain variable bindings

  def referencedVariables = Set.empty

  def referencesContext = false
}

object ConstantOp {
  def fromValue(value: Any) = ConstantOp(DataSet.fromValue(value))

  val empty = ConstantOp(DataSet.empty)
}

/*
 * Reference operations
 */
case class ContextRefOp(types: Set[DataType]) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    bindings.lookupContextVariable.map(DataSet.fromValue(_))
      .getOrElse(throw new WQueryEvaluationException("Context is not bound"))
  }

  def leftType(pos: Int) = if (pos == 0) types else Set.empty

  def rightType(pos: Int) = if (pos == 0) types else Set.empty

  def minTupleSize = 1

  def maxTupleSize = some(1)

  def bindingsPattern = BindingsPattern()

  def referencedVariables = Set.empty

  def referencesContext = true
}

case class PathVariableRefOp(variable: PathVariable, types: (AlgebraOp, Int, Int)) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = bindings.lookupPathVariable(variable.name).map(DataSet.fromTuple(_)).get

  def leftType(pos: Int) = types._1.leftType(pos + types._2)

  def rightType(pos: Int) = types._1.rightType(pos + types._3)

  def minTupleSize = types._1.minTupleSize  - types._2

  def maxTupleSize = types._1.maxTupleSize.map(_ - types._3)

  def bindingsPattern = BindingsPattern()

  def referencedVariables = Set(variable)

  def referencesContext = false
}

case class StepVariableRefOp(variable: StepVariable, types: Set[DataType]) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = bindings.lookupStepVariable(variable.name).map(DataSet.fromValue(_)).get

  def leftType(pos: Int) = if (pos == 0) types else Set.empty

  def rightType(pos: Int) = if (pos == 0) types else Set.empty

  def minTupleSize = 1

  def maxTupleSize = some(1)

  def bindingsPattern = BindingsPattern()

  def referencedVariables = Set(variable)

  def referencesContext = false
}
