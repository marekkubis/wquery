package org.wquery.engine

import org.wquery.WQueryEvaluationException
import org.wquery.model._
import collection.mutable.ListBuffer

sealed abstract class AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings): DataSet
}

/*
 * Imperative operations
 */
case class EmitOp(op: AlgebraOp) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = op.evaluate(wordNet, bindings)
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
}

case class IfElseOp(conditionOp: AlgebraOp, ifOp: AlgebraOp, elseOp: Option[AlgebraOp]) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    if (conditionOp.evaluate(wordNet, bindings).isTrue)
      ifOp.evaluate(wordNet, bindings)
    else
      elseOp.map(_.evaluate(wordNet, bindings)).getOrElse(DataSet.empty)
  }
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
}

case class WhileDoOp(conditionOp: AlgebraOp, iteratedOp: AlgebraOp) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val buffer = new DataSetBuffer

    while (conditionOp.evaluate(wordNet, bindings).isTrue)
      buffer.append(iteratedOp.evaluate(wordNet, bindings))

    buffer.toDataSet
  }
}

/*
 * Set operations
 */
case class UnionOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    DataSet(leftOp.evaluate(wordNet, bindings).paths union rightOp.evaluate(wordNet, bindings).paths)
  }
}

case class ExceptOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val leftSet = leftOp.evaluate(wordNet, bindings)
    val rightSet = rightOp.evaluate(wordNet, bindings)

    DataSet(leftSet.paths.filterNot(rightSet.paths.contains))
  }
}

case class IntersectOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    DataSet(leftOp.evaluate(wordNet, bindings).paths intersect rightOp.evaluate(wordNet, bindings).paths)
  }
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
      val tupleBuffer = new ListBuffer[Any]

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
}

case class ExtendOp(op: AlgebraOp, pos: Int, patterns: List[ExtensionPattern]) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    // TODO consider moving this to the store
    val store = wordNet.store
    val dataSet = op.evaluate(wordNet, bindings)
    val buffer = new DataSetBuffer

    patterns.foreach(pattern => buffer.append(store.extend(dataSet, pattern.relation, pos, pattern.from, pattern.to)))
    buffer.toDataSet
  }
}

case class ExtensionPattern(val relation: Relation, val from: String, val to: List[String])

case class BindOp(op: AlgebraOp, declarations: List[Variable]) extends AlgebraOp with VariableBindings {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    bind(op.evaluate(wordNet, bindings), declarations)
  }
}

/*
 * Elementary operations
 */
case class FetchOp(relation: Relation, from: List[(String, List[Any])], to: List[String]) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = wordNet.store.fetch(relation, from, to)
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
}

object ConstantOp {
  def fromValue(value: Any) = ConstantOp(DataSet.fromValue(value))

  val empty = ConstantOp(DataSet.empty)
}

/*
 * Reference operations
 */
case class ContextRefOp(ref: Int) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    bindings.lookupContextVariable(ref).map(DataSet.fromValue(_))
      .getOrElse(throw new WQueryEvaluationException("Backward reference (" + ref + ") too far"))
  }
}

case class PathVariableRefOp(name: String) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = bindings.lookupPathVariable(name).map(DataSet.fromTuple(_)).get
}

case class StepVariableRefOp(name: String) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = bindings.lookupStepVariable(name).map(DataSet.fromValue(_)).get
}

/*
 * Evaluate operation
 */
case class EvaluateOp(expr: SelfPlannedExpr) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = expr.evaluate(wordNet, bindings)
}
