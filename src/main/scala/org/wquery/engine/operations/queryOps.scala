package org.wquery.engine.operations

import org.wquery.model._
import scalaz._
import Scalaz._
import org.wquery.WQueryEvaluationException
import org.wquery.engine._

sealed abstract class QueryOp extends AlgebraOp

/*
 * Imperative operations
 */
case class EmitOp(op: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = op.evaluate(wordNet, bindings)

  def leftType(pos: Int) = op.leftType(pos)

  def rightType(pos: Int) = op.rightType(pos)

  val minTupleSize = op.minTupleSize

  val maxTupleSize = op.maxTupleSize

  def bindingsPattern = op.bindingsPattern

  val referencedVariables = op.referencedVariables

  val referencesContext = op.referencesContext

  def maxCount(wordNet: WordNetSchema) = op.maxCount(wordNet)
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

  val minTupleSize = iteratedOp.minTupleSize

  val maxTupleSize = iteratedOp.maxTupleSize

  def bindingsPattern = iteratedOp.bindingsPattern

  val referencedVariables = (iteratedOp.referencedVariables -- bindingOp.bindingsPattern.variables) ++ bindingOp.referencedVariables

  val referencesContext = iteratedOp.referencesContext || bindingOp.referencesContext

  def maxCount(wordNet: WordNetSchema) = (bindingOp.maxCount(wordNet) |@| iteratedOp.maxCount(wordNet))(_ * _)
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

  val minTupleSize = elseOp.some(_.minTupleSize.min(ifOp.minTupleSize)).none(ifOp.minTupleSize)

  val maxTupleSize = elseOp.map(_.maxTupleSize.map(elseSize => ifOp.maxTupleSize.map(_.max(elseSize))).getOrElse(none)).getOrElse(ifOp.maxTupleSize)

  def bindingsPattern = elseOp.some(_.bindingsPattern union ifOp.bindingsPattern).none(ifOp.bindingsPattern)

  val referencedVariables = conditionOp.referencedVariables ++ ifOp.referencedVariables ++ elseOp.map(_.referencedVariables).orZero

  val referencesContext = conditionOp.referencesContext || ifOp.referencesContext || elseOp.some(_.referencesContext).none(false)

  def maxCount(wordNet: WordNetSchema) = elseOp.some(op => (op.maxCount(wordNet)<|*|>ifOp.maxCount(wordNet)).map{ case (a,b) => a max b}).none(ifOp.maxCount(wordNet))
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

  val minTupleSize = ops.map(_.minTupleSize).min

  val maxTupleSize = ops.map(_.maxTupleSize).sequence.map(_.sum)

  def bindingsPattern = {
    // It is assumed that all statements in a block provide same binding schemas
    ops.headOption.map(_.bindingsPattern).getOrElse(BindingsPattern())
  }

  val referencedVariables = ops.map(_.referencedVariables).asMA.sum

  val referencesContext = ops.exists(_.referencesContext)

  def maxCount(wordNet: WordNetSchema) = ops.map(_.maxCount(wordNet)).sequence.map(_.sum)
}

case class AssignmentOp(variables: VariableTemplate, op: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val result = op.evaluate(wordNet, bindings)

    if (result.containsSingleTuple) { // TODO remove this constraint
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

  val minTupleSize = 0

  val maxTupleSize = some(0)

  def bindingsPattern = BindingsPattern()

  val referencedVariables = op.referencedVariables

  val referencesContext = op.referencesContext

  def maxCount(wordNet: WordNetSchema) = some(0)
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

  val minTupleSize = iteratedOp.minTupleSize

  val maxTupleSize = iteratedOp.maxTupleSize

  def bindingsPattern = iteratedOp.bindingsPattern

  val referencedVariables = conditionOp.referencedVariables ++ iteratedOp.referencedVariables

  val referencesContext = conditionOp.referencesContext || iteratedOp.referencesContext

  def maxCount(wordNet: WordNetSchema) = none
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

  val minTupleSize = leftOp.minTupleSize min rightOp.minTupleSize

  val maxTupleSize = (leftOp.maxTupleSize |@| rightOp.maxTupleSize)(_ max _)

  def bindingsPattern = BindingsPattern()

  val referencedVariables = leftOp.referencedVariables ++ rightOp.referencedVariables

  val referencesContext = leftOp.referencesContext || rightOp.referencesContext

  def maxCount(wordNet: WordNetSchema) = (leftOp.maxCount(wordNet) |@| rightOp.maxCount(wordNet))(_ max _)
}

case class ExceptOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val leftSet = leftOp.evaluate(wordNet, bindings)
    val rightSet = rightOp.evaluate(wordNet, bindings)

    DataSet(leftSet.paths.filterNot(rightSet.paths.contains))
  }

  def leftType(pos: Int) = leftOp.leftType(pos)

  def rightType(pos: Int) = leftOp.rightType(pos)

  val minTupleSize = leftOp.minTupleSize

  val maxTupleSize = leftOp.maxTupleSize

  def bindingsPattern = BindingsPattern()

  val referencedVariables = leftOp.referencedVariables ++ rightOp.referencedVariables

  val referencesContext = leftOp.referencesContext || rightOp.referencesContext

  def maxCount(wordNet: WordNetSchema) = leftOp.maxCount(wordNet)
}

case class IntersectOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    DataSet(leftOp.evaluate(wordNet, bindings).paths intersect rightOp.evaluate(wordNet, bindings).paths)
  }

  def leftType(pos: Int) = leftOp.leftType(pos) intersect rightOp.leftType(pos)

  def rightType(pos: Int) = leftOp.rightType(pos) intersect rightOp.rightType(pos)

  val minTupleSize = leftOp.minTupleSize max rightOp.minTupleSize

  val maxTupleSize = (leftOp.maxTupleSize |@| rightOp.maxTupleSize)(_ min _)

  def bindingsPattern = BindingsPattern()

  val referencedVariables = leftOp.referencedVariables ++ rightOp.referencedVariables

  val referencesContext = leftOp.referencesContext || rightOp.referencesContext

  def maxCount(wordNet: WordNetSchema) = (leftOp.maxCount(wordNet) |@| rightOp.maxCount(wordNet))(_ min _)
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

  val minTupleSize = leftOp.minTupleSize + rightOp.minTupleSize

  val maxTupleSize = (leftOp.maxTupleSize |@| rightOp.maxTupleSize)(_ + _)

  def bindingsPattern = leftOp.bindingsPattern union rightOp.bindingsPattern

  val referencedVariables = leftOp.referencedVariables ++ rightOp.referencedVariables

  val referencesContext = leftOp.referencesContext || rightOp.referencesContext

  def maxCount(wordNet: WordNetSchema) = (leftOp.maxCount(wordNet) |@| rightOp.maxCount(wordNet))(_ * _)
}

case class NaturalJoinOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends QueryOp {
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

      if (leftTuple.last == rightTuple.head) {
        pathBuffer.append(leftTuple ++ rightTuple.tail)
        leftPathVarNames.foreach(x => pathVarBuffers(x).append(leftSet.pathVars(x)(i)))
        leftStepVarNames.foreach(x => stepVarBuffers(x).append(leftSet.stepVars(x)(i)))

        val offset = leftTuple.size - 1

        rightPathVarNames.foreach { x =>
          val pos = rightSet.pathVars(x)(j)
          pathVarBuffers(x).append((pos._1 + offset, pos._2 + offset))
        }

        for (stepVarName <- rightStepVarNames) {
          val varPos = rightSet.stepVars(stepVarName)(j)

          if (varPos != 0)
            stepVarBuffers(stepVarName).append(varPos + offset)
        }
      }
    }

    DataSet.fromBuffers(pathBuffer, pathVarBuffers, stepVarBuffers)
  }

  def leftType(pos: Int) ={
    val leftMinSize = leftOp.minTupleSize
    val leftMaxSize = leftOp.maxTupleSize

    if (pos < leftMinSize) {
      leftOp.leftType(pos)
    } else if (leftMaxSize.map(pos < _).getOrElse(true)) { // pos < leftMaxSize or leftMaxSize undefined
      val rightOpTypes = for (i <- 0 to pos - leftMinSize) yield rightOp.leftType(i + 1)

      (rightOpTypes :+ leftOp.leftType(pos)).flatten.toSet
    } else { // leftMaxSize defined and pos >= leftMaxSize
      (for (i <- leftMinSize to leftMaxSize.get) yield rightOp.leftType(pos - i + 1)).flatten.toSet
    }
  }

  def rightType(pos: Int) ={
    val rightMinSize = rightOp.minTupleSize
    val rightMaxSize = rightOp.maxTupleSize

    if (pos < rightMinSize) {
      rightOp.rightType(pos)
    } else if (rightMaxSize.map(pos < _).getOrElse(true)) { // pos < rightMaxSize or rightMaxSize undefined
      val leftOpTypes = for (i <- 0 to pos - rightMinSize) yield leftOp.rightType(i + 1)

      (leftOpTypes :+ rightOp.rightType(pos)).flatten.toSet
    } else { // rightMaxSize defined and pos >= rightMaxSize
      (for (i <- rightMinSize to rightMaxSize.get) yield leftOp.rightType(pos - i + 1)).flatten.toSet
    }
  }

  val minTupleSize = leftOp.minTupleSize + rightOp.minTupleSize - 1

  val maxTupleSize = (leftOp.maxTupleSize |@| rightOp.maxTupleSize)(_ + _ - 1)

  def bindingsPattern = leftOp.bindingsPattern union rightOp.bindingsPattern

  val referencedVariables = leftOp.referencedVariables ++ rightOp.referencedVariables

  val referencesContext = leftOp.referencesContext || rightOp.referencesContext

  def maxCount(wordNet: WordNetSchema) = (leftOp.maxCount(wordNet) |@| rightOp.maxCount(wordNet))(_ * _)
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

  val minTupleSize = 1

  val maxTupleSize = some(1)

  def bindingsPattern = BindingsPattern()

  val referencedVariables = leftOp.referencedVariables ++ rightOp.referencedVariables

  val referencesContext = leftOp.referencesContext || rightOp.referencesContext

  def maxCount(wordNet: WordNetSchema) = (leftOp.maxCount(wordNet) |@| rightOp.maxCount(wordNet))(_ * _)
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

  val minTupleSize = 1

  val maxTupleSize = some(1)

  def bindingsPattern = BindingsPattern()

  val referencedVariables = op.referencedVariables

  val referencesContext = op.referencesContext

  def maxCount(wordNet: WordNetSchema) = op.maxCount(wordNet)
}

case class FunctionOp(function: Function, args: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = function.evaluate(args, wordNet, bindings)

  def leftType(pos: Int) = function.leftType(args, pos)

  def rightType(pos: Int) = function.rightType(args, pos)

  val minTupleSize = function.minTupleSize(args)

  val maxTupleSize = function.maxTupleSize(args)

  def bindingsPattern = function.bindingsPattern(args)

  val referencedVariables = args.referencedVariables

  val referencesContext = args.referencesContext

  def maxCount(wordNet: WordNetSchema) = function.maxCount(args, wordNet)
}

/*
 * Declarative operations
 */
case class SelectOp(op: AlgebraOp, condition: Condition) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val dataSet = op.evaluate(wordNet, bindings)
    val pathVarNames = dataSet.pathVars.keySet
    val filterPathVarNames = pathVarNames.filter(pathVarName => condition.referencedVariables.contains(PathVariable(pathVarName)))
    val stepVarNames = dataSet.stepVars.keySet
    val filterStepVarNames = stepVarNames.filter(stepVarName => condition.referencedVariables.contains(StepVariable(stepVarName)))
    val pathBuffer = DataSetBuffers.createPathBuffer
    val pathVarBuffers = DataSetBuffers.createPathVarBuffers(pathVarNames)
    val stepVarBuffers = DataSetBuffers.createStepVarBuffers(stepVarNames)
    val binds = Bindings(bindings, false)

    for (i <- 0 until dataSet.pathCount) {
      val tuple = dataSet.paths(i)

      for (pathVar <- filterPathVarNames) {
        val varPos = dataSet.pathVars(pathVar)(i)
        binds.bindPathVariable(pathVar, tuple.slice(varPos._1, varPos._2))
      }

      for (stepVar <- filterStepVarNames) {
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

  val minTupleSize = op.minTupleSize

  val maxTupleSize = op.maxTupleSize

  def bindingsPattern = op.bindingsPattern

  val referencedVariables = op.referencedVariables ++ (condition.referencedVariables -- op.bindingsPattern.variables)

  val referencesContext = op.referencesContext

  def maxCount(wordNet: WordNetSchema) = op.maxCount(wordNet)
}

case class ProjectOp(op: AlgebraOp, projectOp: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val dataSet = op.evaluate(wordNet, bindings)
    val buffer = new DataSetBuffer
    val pathVarNames = dataSet.pathVars.keys
    val filterPathVarNames = pathVarNames.filter(pathVarName => projectOp.referencedVariables.contains(PathVariable(pathVarName)))
    val stepVarNames = dataSet.stepVars.keys
    val filterStepVarNames = stepVarNames.filter(stepVarName => projectOp.referencedVariables.contains(StepVariable(stepVarName)))
    val binds = Bindings(bindings, false)

    for (i <- 0 until dataSet.pathCount) {
      val tuple = dataSet.paths(i)

      binds.bindContextVariable(tuple.last)

      for (pathVar <- filterPathVarNames) {
        val varPos = dataSet.pathVars(pathVar)(i)
        binds.bindPathVariable(pathVar, tuple.slice(varPos._1, varPos._2))
      }

      for (stepVar <- filterStepVarNames) {
        val varPos = dataSet.stepVars(stepVar)(i)
        binds.bindStepVariable(stepVar, tuple(varPos))
      }

      buffer.append(projectOp.evaluate(wordNet, binds))
    }

    buffer.toDataSet
  }

  def leftType(pos: Int) = projectOp.leftType(pos)

  def rightType(pos: Int) = projectOp.rightType(pos)

  val minTupleSize = projectOp.minTupleSize

  val maxTupleSize = projectOp.maxTupleSize

  def bindingsPattern = projectOp.bindingsPattern

  val referencedVariables = op.referencedVariables ++ (projectOp.referencedVariables -- op.bindingsPattern.variables)

  val referencesContext = op.referencesContext

  def maxCount(wordNet: WordNetSchema) = (op.maxCount(wordNet) |@| projectOp.maxCount(wordNet))(_ * _)
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
          (extensionSize, 1)
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
    if (pos < pattern.minTupleSize) {
      pattern.rightType(pos)
    } else if (pattern.maxTupleSize.map(pos < _).getOrElse(true)) { // pos < maxSize or maxSize undefined
      val extendOpTypes = for (i <- 0 to pos - pattern.minTupleSize) yield op.rightType(i)

      (extendOpTypes :+ pattern.rightType(pos)).flatten.toSet
    } else { // maxSize defined and pos >= maxSize
      (for (i <- pattern.minTupleSize to pattern.maxTupleSize.get) yield op.rightType(pos - i)).flatten.toSet
    }
  }

  val minTupleSize = op.minTupleSize + pattern.minTupleSize

  val maxTupleSize = (op.maxTupleSize |@| pattern.maxTupleSize)(_ + _)

  def bindingsPattern = {
    val pattern = op.bindingsPattern
    pattern.bindVariablesTypes(variables, this)
    pattern
  }

  val referencedVariables = op.referencedVariables

  val referencesContext = op.referencesContext

  def maxCount(wordNet: WordNetSchema) = pattern.maxCount(op.maxCount(wordNet), wordNet)
}

case class BindOp(op: AlgebraOp, variables: VariableTemplate) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    op.evaluate(wordNet, bindings).bindVariables(variables)
  }

  def leftType(pos: Int) = op.leftType(pos)

  def rightType(pos: Int) = op.rightType(pos)

  val minTupleSize = op.minTupleSize

  val maxTupleSize = op.maxTupleSize

  def bindingsPattern = {
    val pattern = op.bindingsPattern
    pattern.bindVariablesTypes(variables, op)
    pattern
  }

  val referencedVariables = op.referencedVariables

  val referencesContext = op.referencesContext

  def maxCount(wordNet: WordNetSchema) = op.maxCount(wordNet)
}

case class FringeOp(pattern: RelationalPattern, side: Side) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = pattern.fringe(wordNet.store, bindings, side)

  def leftType(pos: Int) = (pos == 0).??(pattern.leftType(0))

  def rightType(pos: Int) = (pos == 0).??(pattern.rightType(0))

  val minTupleSize = 1

  val maxTupleSize = some(1)

  def bindingsPattern = BindingsPattern()

  val referencedVariables = ∅[Set[Variable]]

  val referencesContext = false

  def maxCount(wordNet: WordNetSchema) = some(pattern.fringeMaxCount(side, wordNet))
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

  val minTupleSize = to.size

  val maxTupleSize = some(to.size)

  def bindingsPattern = BindingsPattern()

  val referencedVariables = ∅[Set[Variable]]

  val referencesContext = false

  def maxCount(wordNet: WordNetSchema) = some(wordNet.stats.fetchMaxCount(relation, from, to))
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

  val minTupleSize = dataSet.minTupleSize

  val maxTupleSize = dataSet.maxTupleSize

  def bindingsPattern = BindingsPattern() // assumed that a constant dataset does not contain variable bindings

  val referencedVariables = ∅[Set[Variable]]

  val referencesContext = false

  def maxCount(wordNet: WordNetSchema) = some(dataSet.pathCount)
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

  val minTupleSize = 1

  val maxTupleSize = some(1)

  def bindingsPattern = BindingsPattern()

  val referencedVariables = ∅[Set[Variable]]

  val referencesContext = true

  def maxCount(wordNet: WordNetSchema) = some(1)
}

case class PathVariableRefOp(variable: PathVariable, types: (AlgebraOp, Int, Int)) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = bindings.lookupPathVariable(variable.name).map(DataSet.fromTuple(_)).get

  def leftType(pos: Int) = types._1.leftType(pos + types._2)

  def rightType(pos: Int) = types._1.rightType(pos + types._3)

  val minTupleSize = types._1.minTupleSize  - types._2

  val maxTupleSize = types._1.maxTupleSize.map(_ - types._3)

  def bindingsPattern = BindingsPattern()

  val referencedVariables = Set[Variable](variable)

  val referencesContext = false

  def maxCount(wordNet: WordNetSchema) = some(1)
}

case class StepVariableRefOp(variable: StepVariable, types: Set[DataType]) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = bindings.lookupStepVariable(variable.name).map(DataSet.fromValue(_)).get

  def leftType(pos: Int) = if (pos == 0) types else Set.empty

  def rightType(pos: Int) = if (pos == 0) types else Set.empty

  val minTupleSize = 1

  val maxTupleSize = some(1)

  def bindingsPattern = BindingsPattern()

  val referencedVariables = Set[Variable](variable)

  val referencesContext = false

  def maxCount(wordNet: WordNetSchema) = some(1)
}
