package org.wquery.engine

import org.wquery.WQueryEvaluationException
import org.wquery.model._
import collection.mutable.ListBuffer
import java.lang.reflect.Method

sealed abstract class AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings): DataSet
  def rightType(pos: Int): Set[DataType]
  def minTupleSize: Int
  def maxTupleSize: Option[Int]
}

/*
 * Imperative operations
 */
case class EmitOp(op: AlgebraOp) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = op.evaluate(wordNet, bindings)

  def rightType(pos: Int) = op.rightType(pos)

  def minTupleSize = op.minTupleSize

  def maxTupleSize = op.maxTupleSize
}

case class IterateOp(bindingOp: AlgebraOp, iteratedOp: AlgebraOp) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val bindingSet = bindingOp.evaluate(wordNet, bindings)
    val tupleBindings = Bindings(bindings)
    val buffer = new DataSetBuffer
    val pathVarNames = bindingSet.pathVars.keys.toSeq
    val stepVarNames = bindingSet.stepVars.keys.toSeq

    for (i <- 0 until bindingSet.pathCount) {
      val tuple = bindingSet.paths(i)

      pathVarNames.foreach { pathVar =>
        val varPos = bindingSet.pathVars(pathVar)(i)
        tupleBindings.bindPathVariable(pathVar, tuple.slice(varPos._1, varPos._2))
      }

      stepVarNames.foreach(stepVar => tupleBindings.bindStepVariable(stepVar, tuple(bindingSet.stepVars(stepVar)(i))))
      buffer.append(iteratedOp.evaluate(wordNet, tupleBindings))
    }

    buffer.toDataSet
  }

  def rightType(pos: Int) = iteratedOp.rightType(pos)

  def minTupleSize = iteratedOp.minTupleSize

  def maxTupleSize = iteratedOp.maxTupleSize
}

case class IfElseOp(conditionOp: AlgebraOp, ifOp: AlgebraOp, elseOp: Option[AlgebraOp]) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    if (conditionOp.evaluate(wordNet, bindings).isTrue)
      ifOp.evaluate(wordNet, bindings)
    else
      elseOp.map(_.evaluate(wordNet, bindings)).getOrElse(DataSet.empty)
  }

  def rightType(pos: Int) = ifOp.rightType(pos) ++ elseOp.map(_.rightType(pos)).getOrElse(Set.empty)

  def minTupleSize = elseOp.map(_.minTupleSize.min(ifOp.minTupleSize)).getOrElse(ifOp.minTupleSize)

  def maxTupleSize = elseOp.map(_.maxTupleSize.map(elseSize => ifOp.maxTupleSize.map(_.max(elseSize))).getOrElse(None)).getOrElse(ifOp.maxTupleSize)
}

case class BlockOp(ops: List[AlgebraOp]) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val blockBindings = Bindings(bindings)
    val buffer = new DataSetBuffer

    for (op <- ops) {
      buffer.append(op.evaluate(wordNet, blockBindings))
    }

    buffer.toDataSet
  }

  def rightType(pos: Int) = ops.map(_.rightType(pos)).flatten.toSet

  def minTupleSize = ops.map(_.minTupleSize).min

  def maxTupleSize = {
    val opSizes = ops.map(_.maxTupleSize).collect { case Some(x) => x}

    if (opSizes.size != ops.size) None else Some(opSizes.max)
  }
}

case class AssignmentOp(variables: List[Variable], op: AlgebraOp) extends AlgebraOp with VariableBindings {
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

  def rightType(pos: Int) = Set.empty

  def minTupleSize = 0

  def maxTupleSize = Some(0)
}

case class WhileDoOp(conditionOp: AlgebraOp, iteratedOp: AlgebraOp) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val buffer = new DataSetBuffer

    while (conditionOp.evaluate(wordNet, bindings).isTrue)
      buffer.append(iteratedOp.evaluate(wordNet, bindings))

    buffer.toDataSet
  }

  def rightType(pos: Int) = iteratedOp.rightType(pos)

  def minTupleSize = iteratedOp.minTupleSize

  def maxTupleSize = iteratedOp.maxTupleSize
}

/*
 * Set operations
 */
case class UnionOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    DataSet(leftOp.evaluate(wordNet, bindings).paths union rightOp.evaluate(wordNet, bindings).paths)
  }

  def rightType(pos: Int) = leftOp.rightType(pos) ++ rightOp.rightType(pos)

  def minTupleSize = leftOp.minTupleSize min rightOp.minTupleSize

  def maxTupleSize = leftOp.maxTupleSize.map(leftSize => rightOp.maxTupleSize.map(_.max(leftSize))).getOrElse(None)
}

case class ExceptOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val leftSet = leftOp.evaluate(wordNet, bindings)
    val rightSet = rightOp.evaluate(wordNet, bindings)

    DataSet(leftSet.paths.filterNot(rightSet.paths.contains))
  }

  def rightType(pos: Int) = leftOp.rightType(pos)

  def minTupleSize = leftOp.minTupleSize

  def maxTupleSize = leftOp.maxTupleSize
}

case class IntersectOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    DataSet(leftOp.evaluate(wordNet, bindings).paths intersect rightOp.evaluate(wordNet, bindings).paths)
  }

  def rightType(pos: Int) = leftOp.rightType(pos) intersect rightOp.rightType(pos)

  def minTupleSize = leftOp.minTupleSize max rightOp.minTupleSize

  def maxTupleSize = leftOp.maxTupleSize.map(leftSize => rightOp.maxTupleSize.map(_.min(leftSize))).getOrElse(None)
}

case class JoinOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends AlgebraOp {
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
}

/*
 * Arithmetic operations
 */
abstract class BinaryArithmeticOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val leftSet = leftOp.evaluate(wordNet, bindings).paths.map(_.last)
    val rightSet = rightOp.evaluate(wordNet, bindings).paths.map(_.last)

    DataSet(for (leftVal <- leftSet; rightVal <- rightSet) yield List(combine(leftVal, rightVal)))
  }

  def combine(left: Any, right: Any): Any

  def rightType(pos: Int) = if (pos == 0) {
    val leftOpType = leftOp.rightType(pos)
    val rightOpType = rightOp.rightType(pos)

    if (leftOpType == rightOpType) leftOpType else Set(FloatType)
  } else {
    Set.empty
  }

  def minTupleSize = 1

  def maxTupleSize = Some(1)
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

case class MinusOp(op: AlgebraOp) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    DataSet(op.evaluate(wordNet, bindings).paths.map(_.last).map {
        case x: Int => List(-x)
        case x: Double => List(-x)
    })
  }

  def rightType(pos: Int) = op.rightType(pos)

  def minTupleSize = 1

  def maxTupleSize = Some(1)
}

sealed abstract class FunctionOp(function: Function, method: Method, args: AlgebraOp) extends AlgebraOp {
  def rightType(pos: Int) = function.result match {
    case TupleType =>
      DataType.all
    case ValueType(dataType) =>
      if (pos == 0) Set(dataType) else Set.empty
  }

  def minTupleSize = function.result match {
    case TupleType =>
      0
    case ValueType(dataType) =>
      1
  }

  def maxTupleSize = function.result match {
    case TupleType =>
      None
    case ValueType(dataType) =>
      Some(1)
  }
}

case class AggregateFunctionOp(function: Function, method: Method, args: AlgebraOp) extends FunctionOp(function, method, args) {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    method.invoke(WQueryFunctions, args.evaluate(wordNet, bindings)).asInstanceOf[DataSet]
  }
}

case class ScalarFunctionOp(function: Function, method: Method, args: AlgebraOp) extends FunctionOp(function, method, args) {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val argsValues = args.evaluate(wordNet, bindings)
    val buffer = new ListBuffer[List[Any]]()
    val invocationArgs = new Array[AnyRef](function.args.size)

    for (tuple <- argsValues.paths) {
      for (i <- 0 until invocationArgs.size)
        invocationArgs(i) = tuple(i).asInstanceOf[AnyRef]

      buffer.append(List(method.invoke(WQueryFunctions, invocationArgs: _*)))
    }

    DataSet(buffer.toList)
  }
}

/*
 * Declarative operations
 */
case class SelectOp(op: AlgebraOp, condition: ConditionalExpr) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val dataSet = op.evaluate(wordNet, bindings)
    val pathVarNames = dataSet.pathVars.keys.toSeq
    val stepVarNames = dataSet.stepVars.keys.toSeq
    val pathBuffer = DataSetBuffers.createPathBuffer
    val pathVarBuffers = DataSetBuffers.createPathVarBuffers(pathVarNames)
    val stepVarBuffers = DataSetBuffers.createStepVarBuffers(stepVarNames)

    // TODO OPT here determine which variables are to be used by filter

    for (i <- 0 until dataSet.pathCount) {
      val tuple = dataSet.paths(i)
      val binds = Bindings(bindings)

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

  def rightType(pos: Int) = op.rightType(pos)

  def minTupleSize = op.minTupleSize

  def maxTupleSize = op.maxTupleSize
}

case class ProjectOp(op: AlgebraOp, projectOp: AlgebraOp) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val dataSet = op.evaluate(wordNet, bindings)
    val buffer = new DataSetBuffer
    val pathVarNames = dataSet.pathVars.keys
    val stepVarNames = dataSet.stepVars.keys
    val binds = Bindings(bindings)

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

  def rightType(pos: Int) = projectOp.rightType(pos)

  def minTupleSize = projectOp.minTupleSize

  def maxTupleSize = projectOp.maxTupleSize
}

case class ExtendOp(op: AlgebraOp, pattern: ExtensionPattern) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    wordNet.store.extend(op.evaluate(wordNet, bindings), pattern)
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
}

case class CloseOp(op: AlgebraOp, patterns: List[ExtensionPattern], limit: Option[Int]) extends AlgebraOp {
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

  def rightType(pos: Int) = {
    op.rightType(pos) ++ types.filter(t => t.isDefinedAt(t.size - 1 - pos)).map(t => t(t.size - 1 - pos))
  }

  private def crossTypes(leftTypes: List[List[DataType]], rightTypes: List[List[DataType]]) = {
    for (left <- leftTypes; right <- rightTypes)
      yield left ++ right
  }

  def minTupleSize = op.minTupleSize

  def maxTupleSize = None
}

case class BindOp(op: AlgebraOp, declarations: List[Variable]) extends AlgebraOp with VariableBindings {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    bind(op.evaluate(wordNet, bindings), declarations)
  }

  def rightType(pos: Int) = op.rightType(pos)

  def minTupleSize = op.minTupleSize

  def maxTupleSize = op.maxTupleSize
}

/*
 * Elementary operations
 */
case class FetchOp(relation: Relation, from: List[(String, List[Any])], to: List[String]) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = wordNet.store.fetch(relation, from, to)

  def rightType(pos: Int) = {
    val args = from.map(_._1) ++ to
    val index = args.size - 1 - pos

    if (args.isDefinedAt(index))
      Set(relation.demandArgument(args(index)))
    else
      Set.empty
  }

  def minTupleSize = from.size + to.size

  def maxTupleSize = Some(from.size + to.size)
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

case class ConstantOp(dataSet: DataSet) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = dataSet

  def rightType(pos: Int) = dataSet.getType(pos)

  def minTupleSize = dataSet.minTupleSize

  def maxTupleSize = Some(dataSet.maxTupleSize)
}

object ConstantOp {
  def fromValue(value: Any) = ConstantOp(DataSet.fromValue(value))

  val empty = ConstantOp(DataSet.empty)
}

/*
 * Reference operations
 */
case class ContextRefOp(ref: Int, types: Set[DataType]) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    bindings.lookupContextVariable(ref).map(DataSet.fromValue(_))
      .getOrElse(throw new WQueryEvaluationException("Backward reference (" + ref + ") too far"))
  }

  def rightType(pos: Int) = if (pos == 0) types else Set.empty

  def minTupleSize = 1

  def maxTupleSize = Some(1)
}

case class PathVariableRefOp(name: String, types: List[DataType]) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = bindings.lookupPathVariable(name).map(DataSet.fromTuple(_)).get

  def rightType(pos: Int) = if (types.isDefinedAt(types.size - 1 - pos)) Set(types(types.size - 1 - pos)) else Set.empty

  def minTupleSize = types.size

  def maxTupleSize = Some(types.size)
}

case class StepVariableRefOp(name: String, dataType: DataType) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = bindings.lookupStepVariable(name).map(DataSet.fromValue(_)).get

  def rightType(pos: Int) = if (pos == 0) Set(dataType) else Set.empty

  def minTupleSize = 1

  def maxTupleSize = Some(1)
}

/*
 * Evaluate operation
 */
case class EvaluateOp(expr: SelfPlannedExpr, wordNet: WordNet, bindings: Bindings) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = expr.evaluate(wordNet, bindings)

  def rightType(pos: Int) = {
    val types = expr.evaluate(wordNet, bindings).getType(pos)

    if (types.isEmpty) DataType.all else types // TODO temporary - to be removed
  }

  def minTupleSize = expr.evaluate(wordNet, bindings).minTupleSize

  def maxTupleSize = Some(expr.evaluate(wordNet, bindings).maxTupleSize)
}
