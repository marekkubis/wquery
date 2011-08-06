package org.wquery.engine

import org.wquery.WQueryEvaluationException
import org.wquery.model._
import collection.mutable.ListBuffer

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
}

case class IfElseOp(conditionOp: AlgebraOp, ifOp: AlgebraOp, elseOp: Option[AlgebraOp]) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    if (conditionOp.evaluate(wordNet, bindings).isTrue)
      ifOp.evaluate(wordNet, bindings)
    else
      elseOp.map(_.evaluate(wordNet, bindings)).getOrElse(DataSet.empty)
  }

  def leftType(pos: Int) = ifOp.leftType(pos) ++ elseOp.map(_.leftType(pos)).getOrElse(Set.empty)

  def rightType(pos: Int) = ifOp.rightType(pos) ++ elseOp.map(_.rightType(pos)).getOrElse(Set.empty)

  def minTupleSize = elseOp.map(_.minTupleSize.min(ifOp.minTupleSize)).getOrElse(ifOp.minTupleSize)

  def maxTupleSize = elseOp.map(_.maxTupleSize.map(elseSize => ifOp.maxTupleSize.map(_.max(elseSize))).getOrElse(None)).getOrElse(ifOp.maxTupleSize)

  def bindingsPattern = elseOp.map(_.bindingsPattern union ifOp.bindingsPattern).getOrElse(ifOp.bindingsPattern)
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

    if (opSizes.size != ops.size) None else Some(opSizes.max)
  }

  def bindingsPattern = {
    // It is assumed that all statements in a block provide same binding schemas
    ops.headOption.map(_.bindingsPattern).getOrElse(BindingsPattern())
  }
}

case class AssignmentOp(variables: List[Variable], op: AlgebraOp) extends QueryOp with VariableBindings {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val result = op.evaluate(wordNet, bindings)

    if (result.pathCount == 1) { // TODO remove this constraint
      val tuple = result.paths.head
      val dataSet = bind(DataSet.fromTuple(tuple), variables)

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

  def maxTupleSize = Some(0)

  def bindingsPattern = BindingsPattern()
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

  def maxTupleSize = leftOp.maxTupleSize.map(leftSize => rightOp.maxTupleSize.map(_.max(leftSize))).getOrElse(None)

  def bindingsPattern = BindingsPattern()
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
}

case class IntersectOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    DataSet(leftOp.evaluate(wordNet, bindings).paths intersect rightOp.evaluate(wordNet, bindings).paths)
  }

  def leftType(pos: Int) = leftOp.leftType(pos) intersect rightOp.leftType(pos)

  def rightType(pos: Int) = leftOp.rightType(pos) intersect rightOp.rightType(pos)

  def minTupleSize = leftOp.minTupleSize max rightOp.minTupleSize

  def maxTupleSize = leftOp.maxTupleSize.map(leftSize => rightOp.maxTupleSize.map(_.min(leftSize))).getOrElse(None)

  def bindingsPattern = BindingsPattern()
}

case class JoinOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val leftSet = leftOp.evaluate(wordNet, bindings)
    val rightSet = rightOp.evaluate(wordNet, bindings)
    val leftPathVarNames = leftSet.pathVars.keys.toSeq
    val leftStepVarNames = leftSet.stepVars.keys.toSeq
    val rightPathVarNames = rightSet.pathVars.keys.toSeq
    val rightStepVarNames = rightSet.stepVars.keys.toSeq
    val pathBuffer = DataSetBuffers.createPathBuffer
    val pathVarBuffers = DataSetBuffers.createPathVarBuffers(leftPathVarNames ++ rightPathVarNames)
    val stepVarBuffers = DataSetBuffers.createStepVarBuffers(leftStepVarNames ++ rightStepVarNames)

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

  def maxTupleSize = leftOp.maxTupleSize.map(leftSize => rightOp.maxTupleSize.map(_ + leftSize)).getOrElse(None)

  def bindingsPattern = leftOp.bindingsPattern union rightOp.bindingsPattern
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

  def maxTupleSize = Some(1)

  def bindingsPattern = BindingsPattern()
}

case class AddOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends BinaryArithmeticOp(leftOp, rightOp) {
  def combine(leftVal: Any, rightVal: Any) = (leftVal, rightVal) match {
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
  def combine(leftVal: Any, rightVal: Any) = (leftVal, rightVal) match {
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
  def combine(leftVal: Any, rightVal: Any) = (leftVal, rightVal) match {
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
  def combine(leftVal: Any, rightVal: Any) = (leftVal, rightVal) match {
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
  def combine(leftVal: Any, rightVal: Any) = (leftVal, rightVal) match {
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

  def maxTupleSize = Some(1)

  def bindingsPattern = BindingsPattern()
}

case class FunctionOp(function: Function, args: AlgebraOp) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = function.evaluate(args, wordNet, bindings)

  def leftType(pos: Int) = function.leftType(args, pos)

  def rightType(pos: Int) = function.rightType(args, pos)

  def minTupleSize = function.minTupleSize(args)

  def maxTupleSize = function.maxTupleSize(args)

  def bindingsPattern = function.bindingsPattern(args)
}

/*
 * Declarative operations
 */
case class SelectOp(op: AlgebraOp, condition: Condition) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val dataSet = op.evaluate(wordNet, bindings)
    val pathVarNames = dataSet.pathVars.keys.toSeq
    val stepVarNames = dataSet.stepVars.keys.toSeq
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

      binds.bindContextVariables(tuple)

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

      binds.bindContextVariables(tuple)

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
}

case class ExtendOp(op: AlgebraOp, pattern: ExtensionPattern) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    wordNet.store.extend(op.evaluate(wordNet, bindings), pattern)
  }

  def leftType(pos: Int) = {
    if (pos < op.minTupleSize) {
      op.leftType(pos)
    } else if (op.maxTupleSize.map(pos < _).getOrElse(true)) { // pos < maxTupleSize or maxTupleSize undefined
      val extendOpTypes = for (i <- 0 to pos - op.minTupleSize) yield pattern.destinationTypeAt(i)

      (extendOpTypes :+ op.leftType(pos)).flatten.toSet
    } else { // maxTupleSize defined and pos >= maxTupleSize
      (for (i <- op.minTupleSize to op.maxTupleSize.get) yield pattern.destinationTypeAt(pos - i)).flatten.toSet
    }
  }

  def rightType(pos: Int) = {
    pattern.destinationTypes.flatMap { types =>
      if (types.isDefinedAt(types.size - 1 - pos))
        Set(types(types.size - 1 - pos))
      else
        op.rightType(pos - types.size)
    }.toSet
  }

  def minTupleSize = op.minTupleSize + pattern.minDestinationTypesSize

  def maxTupleSize = op.maxTupleSize.map(_ + pattern.maxDestinationTypesSize)

  def bindingsPattern = op.bindingsPattern
}

case class CloseOp(op: AlgebraOp, patterns: List[ExtensionPattern], limit: Option[Int]) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val dataSet = op.evaluate(wordNet, bindings)
    val pathVarNames = dataSet.pathVars.keys.toSeq
    val stepVarNames = dataSet.stepVars.keys.toSeq
    val pathBuffer = DataSetBuffers.createPathBuffer
    val pathVarBuffers = DataSetBuffers.createPathVarBuffers(pathVarNames)
    val stepVarBuffers = DataSetBuffers.createStepVarBuffers(stepVarNames)

    for (i <- 0 until dataSet.pathCount) {
      val tuple = dataSet.paths(i)

      pathBuffer.append(tuple)
      pathVarNames.foreach(x => pathVarBuffers(x).append(dataSet.pathVars(x)(i)))
      stepVarNames.foreach(x => stepVarBuffers(x).append(dataSet.stepVars(x)(i)))

      for (cnode <- closure(wordNet, bindings, tuple, Set(tuple.last), limit)) {
        pathBuffer.append(cnode)
        pathVarNames.foreach(x => pathVarBuffers(x).append(dataSet.pathVars(x)(i)))
        stepVarNames.foreach(x => stepVarBuffers(x).append(dataSet.stepVars(x)(i)))
      }
    }

    DataSet.fromBuffers(pathBuffer, pathVarBuffers, stepVarBuffers)
  }

  private def closure(wordNet: WordNet, bindings: Bindings, source: List[Any], forbidden: Set[Any], limit: Option[Int]): List[List[Any]] = {
    if (limit.getOrElse(1) > 0) {
      val store = wordNet.store
      val transformed = patterns.foldLeft(DataSet.fromTuple(source))((dataSet, extension) => store.extend(dataSet, extension))

      val filtered = transformed.paths.filter { x => !forbidden.contains(x.last) }

      if (filtered.isEmpty) {
        filtered
      } else {
        val result = new ListBuffer[List[Any]]
        val newForbidden = forbidden ++ filtered.map(_.last)
	      val newLimit = limit.map(_ - 1)

        result.appendAll(filtered)

        filtered.foreach { x =>
          result.appendAll(closure(wordNet, bindings, x, newForbidden, newLimit))
        }

        result.toList
      }
    } else {
      Nil
    }
  }

  val types = patterns.tail.foldLeft(patterns.head.destinationTypes)((l, r) => crossTypes(l, r.destinationTypes))

  def leftType(pos: Int) = {
    if (pos < op.minTupleSize) {
      op.leftType(pos)
    } else if (op.maxTupleSize.map(pos < _).getOrElse(true)) { // pos < maxTupleSize or maxTupleSize undefined
      val extendOpTypes = for (i <- 0 to pos - op.minTupleSize) yield patterns.map(_.destinationTypeAt(i)).flatten.toSet

      (extendOpTypes :+ op.leftType(pos)).flatten.toSet
    } else { // maxTupleSize defined and pos >= maxTupleSize
      (for (i <- op.minTupleSize to op.maxTupleSize.get) yield patterns.map(_.destinationTypeAt(pos - i)).flatten).flatten.toSet
    }
  }

  def rightType(pos: Int) = {
    op.rightType(pos) ++ types.filter(t => t.isDefinedAt(t.size - 1 - pos)).map(t => t(t.size - 1 - pos))
  }

  private def crossTypes(leftTypes: List[List[DataType]], rightTypes: List[List[DataType]]) = {
    for (left <- leftTypes; right <- rightTypes)
      yield left ++ right
  }

  def minTupleSize = op.minTupleSize

  def maxTupleSize = None

  def bindingsPattern = op.bindingsPattern
}

case class BindOp(op: AlgebraOp, variables: List[Variable]) extends QueryOp with VariableBindings with VariableTypeBindings {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    bind(op.evaluate(wordNet, bindings), variables)
  }

  def leftType(pos: Int) = op.leftType(pos)

  def rightType(pos: Int) = op.rightType(pos)

  def minTupleSize = op.minTupleSize

  def maxTupleSize = op.maxTupleSize

  def bindingsPattern = {
    val pattern = op.bindingsPattern
    bindTypes(pattern, op, variables)
    pattern
  }
}

trait VariableBindings {
  def bind(dataSet: DataSet, variables: List[Variable]) = {
    if (variables != Nil) {
      val pathVarBuffers = DataSetBuffers.createPathVarBuffers(variables.filter(x => (x.isInstanceOf[PathVariable] && x.name != "_")).map(_.name))
      val stepVarBuffers = DataSetBuffers.createStepVarBuffers(variables.filterNot(x => (x.isInstanceOf[PathVariable] || x.name == "_")).map(_.name))

      demandUniqueVariableNames(variables)

      getPathVariablePosition(variables) match {
        case Some(pathVarPos) =>
          val leftVars = variables.slice(0, pathVarPos).map(_.name).zipWithIndex.filterNot{_._1 == "_"}.toMap
          val rightVars = variables.slice(pathVarPos + 1, variables.size).map(_.name).reverse.zipWithIndex.filterNot{_._1 == "_"}.toMap
          val pathVarBuffer = if (variables(pathVarPos).name != "_") Some(pathVarBuffers(variables(pathVarPos).name)) else None
          val pathVarStart = leftVars.size
          val pathVarEnd = rightVars.size

          for (tuple <- dataSet.paths) {
            dataSet.paths.foreach(tuple => bindVariablesFromRight(rightVars, stepVarBuffers, tuple.size))
            pathVarBuffer.map(_.append((pathVarStart, tuple.size - pathVarEnd)))
            dataSet.paths.foreach(tuple => bindVariablesFromLeft(leftVars, stepVarBuffers, tuple.size))
          }
        case None =>
          val rightVars = variables.map(_.name).reverse.zipWithIndex.filterNot{_._1 == "_"}.toMap
          dataSet.paths.foreach(tuple => bindVariablesFromRight(rightVars, stepVarBuffers, tuple.size))
      }

      DataSet(dataSet.paths, dataSet.pathVars ++ pathVarBuffers.mapValues(_.toList), dataSet.stepVars ++ stepVarBuffers.mapValues(_.toList))
    } else {
      dataSet
    }
  }

  private def demandUniqueVariableNames(variables: List[Variable]) {
    val variableNames = variables.filter(x => !x.isInstanceOf[PathVariable] && x.name != "_").map(_.name)

    if (variableNames.size != variableNames.distinct.size)
      throw new WQueryEvaluationException("Variable list contains duplicated variable names")
  }

  private def getPathVariablePosition(variables: List[Variable]) = {
    val pathVarPos = variables.indexWhere{_.isInstanceOf[PathVariable]}

    if (pathVarPos != variables.lastIndexWhere{_.isInstanceOf[PathVariable]}) {
      throw new WQueryEvaluationException("Variable list '" + variables.map {
        case PathVariable(v) => "@" + v
        case StepVariable(v) => "$" + v
      }.mkString + "' contains more than one path variable")
    } else {
      if (pathVarPos != -1)
        Some(pathVarPos)
      else
        None
    }
  }

  private def bindVariablesFromLeft(vars: Map[String, Int], varIndexes: Map[String, ListBuffer[Int]], tupleSize: Int) {
    for ((v, pos) <- vars)
      if (pos < tupleSize)
        varIndexes(v).append(pos)
      else
        throw new WQueryEvaluationException("Variable $" + v + " cannot be bound")
  }

  private def bindVariablesFromRight(vars: Map[String, Int], varIndexes: Map[String, ListBuffer[Int]], tupleSize: Int) {
    for ((v, pos) <- vars) {
      val index = tupleSize - 1 - pos

      if (index >= 0)
        varIndexes(v).append(index)
      else
        throw new WQueryEvaluationException("Variable $" + v + " cannot be bound")
    }
  }
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
      Set(relation.demandArgument(args(pos)))
    else
      Set.empty
  }

  def minTupleSize = to.size

  def maxTupleSize = Some(to.size)

  def bindingsPattern = BindingsPattern()
}

object FetchOp {
  def words = FetchOp(WordNet.WordSet, List((Relation.Source, Nil)), List(Relation.Source))

  def senses = FetchOp(WordNet.SenseSet, List((Relation.Source, Nil)), List(Relation.Source))

  def synsets = FetchOp(WordNet.SynsetSet, List((Relation.Source, Nil)), List(Relation.Source))

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

  def relationTuplesByArgumentNames(relation: Relation, argumentNames: List[String])
    = FetchOp(relation, argumentNames.map(x => (x, List[Any]())), argumentNames)
}

case class ConstantOp(dataSet: DataSet) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = dataSet

  def leftType(pos: Int) = dataSet.leftType(pos)

  def rightType(pos: Int) = dataSet.rightType(pos)

  def minTupleSize = dataSet.minTupleSize

  def maxTupleSize = Some(dataSet.maxTupleSize)

  def bindingsPattern = BindingsPattern() // assumed that constant dataset does not contain variable bindings
}

object ConstantOp {
  def fromValue(value: Any) = ConstantOp(DataSet.fromValue(value))

  val empty = ConstantOp(DataSet.empty)
}

/*
 * Reference operations
 */
case class ContextRefOp(ref: Int, types: Set[DataType]) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    bindings.lookupContextVariable(ref).map(DataSet.fromValue(_))
      .getOrElse(throw new WQueryEvaluationException("Backward reference (" + ref + ") too far"))
  }

  def leftType(pos: Int) = if (pos == 0) types else Set.empty

  def rightType(pos: Int) = if (pos == 0) types else Set.empty

  def minTupleSize = 1

  def maxTupleSize = Some(1)

  def bindingsPattern = BindingsPattern()
}

case class PathVariableRefOp(name: String, types: (AlgebraOp, Int, Int)) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = bindings.lookupPathVariable(name).map(DataSet.fromTuple(_)).get

  def leftType(pos: Int) = types._1.leftType(pos + types._2)

  def rightType(pos: Int) = types._1.rightType(pos + types._3)

  def minTupleSize = types._1.minTupleSize  - types._2

  def maxTupleSize = types._1.maxTupleSize.map(_ - types._3)

  def bindingsPattern = BindingsPattern()
}

case class StepVariableRefOp(name: String, types: Set[DataType]) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = bindings.lookupStepVariable(name).map(DataSet.fromValue(_)).get

  def leftType(pos: Int) = if (pos == 0) types else Set.empty

  def rightType(pos: Int) = if (pos == 0) types else Set.empty

  def minTupleSize = 1

  def maxTupleSize = Some(1)

  def bindingsPattern = BindingsPattern()
}
