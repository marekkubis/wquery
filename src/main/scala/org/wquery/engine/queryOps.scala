package org.wquery.engine

import org.wquery.model._
import collection.mutable.ListBuffer
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
      elseOp.map(_.evaluate(wordNet, bindings)).getOrElse(DataSet.empty)
  }

  def leftType(pos: Int) = ifOp.leftType(pos) ++ elseOp.map(_.leftType(pos)).getOrElse(Set.empty)

  def rightType(pos: Int) = ifOp.rightType(pos) ++ elseOp.map(_.rightType(pos)).getOrElse(Set.empty)

  def minTupleSize = elseOp.map(_.minTupleSize.min(ifOp.minTupleSize)).getOrElse(ifOp.minTupleSize)

  def maxTupleSize = elseOp.map(_.maxTupleSize.map(elseSize => ifOp.maxTupleSize.map(_.max(elseSize))).getOrElse(None)).getOrElse(ifOp.maxTupleSize)

  def bindingsPattern = elseOp.map(_.bindingsPattern union ifOp.bindingsPattern).getOrElse(ifOp.bindingsPattern)

  def referencedVariables = conditionOp.referencedVariables ++ ifOp.referencedVariables ++ elseOp.map(_.referencedVariables).getOrElse(Set.empty)

  def referencesContext = conditionOp.referencesContext || ifOp.referencesContext || elseOp.map(_.referencesContext).getOrElse(false)
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

  def referencedVariables = ops.headOption
    .map(head => ops.tail.foldLeft(head.referencedVariables)((l,r) => l ++ r.referencedVariables))
    .getOrElse(Set.empty)

  def referencesContext = ops.exists(_.referencesContext)
}

case class AssignmentOp(variables: VariableTemplate, op: AlgebraOp) extends QueryOp with VariableBindings {
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

  def maxTupleSize = leftOp.maxTupleSize.map(leftSize => rightOp.maxTupleSize.map(_.max(leftSize))).getOrElse(None)

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

  def maxTupleSize = leftOp.maxTupleSize.map(leftSize => rightOp.maxTupleSize.map(_.min(leftSize))).getOrElse(None)

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

  def maxTupleSize = leftOp.maxTupleSize.map(leftSize => rightOp.maxTupleSize.map(_ + leftSize)).getOrElse(None)

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

  def maxTupleSize = Some(1)

  def bindingsPattern = BindingsPattern()

  def referencedVariables = leftOp.referencedVariables ++ rightOp.referencedVariables

  def referencesContext = leftOp.referencesContext || rightOp.referencesContext
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

  def referencedVariables = op.referencedVariables ++ (projectOp.referencedVariables -- op.bindingsPattern.variables)

  def referencesContext = op.referencesContext
}

case class ExtendOp(op: AlgebraOp, from: Int, pattern: RelationalPattern, variables: VariableTemplate) extends QueryOp with VariableTypeBindings {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val dataSet = op.evaluate(wordNet, bindings)
    val extensionSet = pattern.extend(wordNet.store, bindings, new DataExtensionSet(dataSet), from)
    val dataSetPathVarNames = dataSet.pathVars.keySet
    val dataSetStepVarNames = dataSet.stepVars.keySet
    val pathBuffer = DataSetBuffers.createPathBuffer
    val pathVarBuffers = DataSetBuffers.createPathVarBuffers(variables.pathVariableName.map(dataSetPathVarNames + _).getOrElse(dataSetPathVarNames))
    val stepVarBuffers = DataSetBuffers.createStepVarBuffers(dataSetStepVarNames union variables.stepVariableNames)

    for (head::extension <- extensionSet.extensions) {
      val pathPos = head.asInstanceOf[Int]
      val path = dataSet.paths(pathPos)
      val pathSize = path.size
      val extensionSize = extension.size
      
      pathBuffer.append(path ++ extension)

      for (pathVar <- dataSetPathVarNames)
        pathVarBuffers(pathVar).append(dataSet.pathVars(pathVar)(pathPos))

      for (stepVar <- dataSetStepVarNames)
        stepVarBuffers(stepVar).append(dataSet.stepVars(stepVar)(pathPos))

      for (pathVar <- variables.pathVariableName)
        pathVarBuffers(pathVar).append(variables.pathVariableIndexes(extensionSize, pathSize))

      for (stepVar <- variables.leftVariablesNames)
        stepVarBuffers(stepVar).append(variables.leftIndex(stepVar, extensionSize, pathSize))

      for (stepVar <- variables.rightVariablesNames)
        stepVarBuffers(stepVar).append(variables.rightIndex(stepVar, extensionSize, pathSize))
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

  def maxTupleSize = op.maxTupleSize.map(maxTupleSize => pattern.maxSize.map(maxSize => maxTupleSize + maxSize)).getOrElse(None)

  def bindingsPattern = {
    val pattern = op.bindingsPattern
    bindTypes(pattern, this, variables)
    pattern
  }

  def referencedVariables = op.referencedVariables

  def referencesContext = op.referencesContext
}

sealed abstract class RelationalPattern {
  def extend(wordNet: WordNetStore, bindings: Bindings, extensionSet: ExtensionSet, from: Int): ExtendedExtensionSet

  def minSize: Int

  def maxSize: Option[Int]

  def sourceType: Set[DataType]

  def leftType(pos: Int): Set[DataType]

  def rightType(pos: Int): Set[DataType]
}

case class RelationUnionPattern(patterns: List[RelationalPattern]) extends RelationalPattern {
  def extend(wordNet: WordNetStore, bindings: Bindings, extensionSet: ExtensionSet, from: Int) = {
    val buffer = new ExtensionSetBuffer(extensionSet)

    patterns.foreach(expr => buffer.append(expr.extend(wordNet, bindings, extensionSet, from)))
    buffer.toExtensionSet
  }

  def minSize = patterns.map(_.minSize).min

  def maxSize = if (patterns.exists(!_.maxSize.isDefined)) None else patterns.map(_.maxSize).max

  def sourceType = patterns.flatMap(_.sourceType).toSet

  def leftType(pos: Int) = patterns.flatMap(_.leftType(pos)).toSet

  def rightType(pos: Int) = patterns.flatMap(_.rightType(pos)).toSet
}

case class RelationCompositionPattern(patterns: List[RelationalPattern]) extends RelationalPattern {
  def extend(wordNet: WordNetStore, bindings: Bindings, extensionSet: ExtensionSet, from: Int) = {
    val headSet = patterns.head.extend(wordNet, bindings, extensionSet, from)

    patterns.tail.foldLeft(headSet)((dataSet, expr) => expr.extend(wordNet, bindings, dataSet, 0))
  }

  def minSize = patterns.map(_.minSize).sum

  def maxSize = {
    if (patterns.exists(!_.maxSize.isDefined))
      None
    else
      Some(patterns.map(_.maxSize).collect{ case Some(num) => num }.sum)
  }

  def sourceType = patterns.head.sourceType

  def leftType(pos: Int) = leftType(patterns, pos)

  private def leftType(patterns: List[RelationalPattern], pos: Int): Set[DataType] = {
    patterns.headOption.map { headPattern =>
      if (pos < headPattern.minSize)
        headPattern.leftType(pos)
      else if (headPattern.maxSize.map(pos < _).getOrElse(true)) { // pos < maxSize or maxSize undefined
        headPattern.leftType(pos) union leftType(patterns.tail, pos - headPattern.minSize)
      } else { // else pos < maxSize and defined
        leftType(patterns.tail, pos - headPattern.maxSize.get)
      }
    }.getOrElse(Set.empty)
  }

  def rightType(pos: Int) = rightType(patterns.reverse, pos)

  private def rightType(patterns: List[RelationalPattern], pos: Int): Set[DataType] = {
    patterns.headOption.map { headPattern =>
      if (pos < headPattern.minSize)
        headPattern.rightType(pos)
      else if (headPattern.maxSize.map(pos < _).getOrElse(true)) { // pos < maxSize or maxSize undefined
        headPattern.rightType(pos) union rightType(patterns.tail, pos - headPattern.minSize)
      } else { // else pos < maxSize and defined
        rightType(patterns.tail, pos - headPattern.maxSize.get)
      }
    }.getOrElse(Set.empty)
  }
}

case class QuantifiedRelationPattern(pattern: RelationalPattern, quantifier: Quantifier) extends RelationalPattern {
  def extend(wordNet: WordNetStore, bindings: Bindings, extensionSet: ExtensionSet, from: Int) = {
    val lowerExtensionSet = (1 to quantifier.lowerBound).foldLeft(extensionSet)((x, _) => pattern.extend(wordNet, bindings, x, from))

    if (Some(quantifier.lowerBound) != quantifier.upperBound)
      computeClosure(wordNet, bindings, lowerExtensionSet, from, quantifier.upperBound.map(_ - quantifier.lowerBound))
    else
      lowerExtensionSet.asInstanceOf[ExtendedExtensionSet] // valid quantifiers invoke extend at least once
  }

  private def computeClosure(wordNet: WordNetStore, bindings: Bindings, extensionSet: ExtensionSet, from: Int, limit: Option[Int]) = {
    val builder = new ExtensionSetBuilder(extensionSet)

    for (i <- 0 until extensionSet.size) {
      val source = extensionSet.right(i, from)
      builder.extend(i, Nil)

      for (cnode <- closeTuple(wordNet, bindings, List(source), Set(source), limit))
        builder.extend(i, cnode)
    }

    builder.build
  }

  private def closeTuple(wordNet: WordNetStore, bindings: Bindings, source: List[Any], forbidden: Set[Any], limit: Option[Int]): Seq[List[Any]] = {
    if (limit.getOrElse(1) > 0) {
      val transformed = pattern.extend(wordNet, bindings, new DataExtensionSet(DataSet.fromList(source)), 0)
      val filtered = transformed.extensions.filter(x => !forbidden.contains(x.last)).map(_.tail)

      if (filtered.isEmpty) {
        filtered
      } else {
        val result = new ListBuffer[List[Any]]
        val newForbidden = forbidden ++ filtered.map(_.last)
	      val newLimit = limit.map(_ - 1)

        for (extension <- filtered) {
          val newSource = source ++ extension

          result.append(newSource.tail)
          result.appendAll(closeTuple(wordNet, bindings, newSource, newForbidden, newLimit))
        }

        result
      }
    } else {
      Nil
    }
  }

  def minSize = pattern.minSize * quantifier.lowerBound

  def maxSize = pattern.maxSize.map(maxSize => quantifier.upperBound.map(upperBound => maxSize * upperBound)).getOrElse(None)

  def sourceType = pattern.sourceType

  def leftType(pos: Int) = if (maxSize.map(pos < _).getOrElse(true)) {
    (for (i <- pattern.minSize to pattern.maxSize.getOrElse(pos + 1))
      yield pattern.leftType(if (i > 0) pos % i else 0)).flatten.toSet
  } else {
    Set[DataType]()
  }

  def rightType(pos: Int) = if (maxSize.map(pos < _).getOrElse(true)) {
    (for (i <- pattern.minSize to pattern.maxSize.getOrElse(pos + 1))
      yield pattern.rightType(if (i > 0) pos % i else 0)).flatten.toSet
  } else {
    Set[DataType]()
  }
}

case class Quantifier(lowerBound: Int, upperBound: Option[Int]) {
  override def toString = "{" + lowerBound + upperBound.map(ub => if (lowerBound == ub) "}" else "," + ub  + "}").getOrElse(",}")
}

case class VariableRelationalPattern(variable: StepVariable) extends RelationalPattern {
  def extend(wordNet: WordNetStore, bindings: Bindings, extensionSet: ExtensionSet, from: Int) = {
    bindings.lookupStepVariable(variable.name).map {
      case Arc(relation, source, destination) =>
        wordNet.extend(extensionSet, relation, from, source, List(destination))
      case _ =>
        throw new WQueryEvaluationException("Cannot extend a path using a non-arc value of variable " + variable)
    }.getOrElse(throw new WQueryEvaluationException("Variable " + variable + " is not bound"))
  }

  def minSize = 2

  def maxSize = Some(2)

  def sourceType = DataType.all

  def leftType(pos: Int) = pos match {
    case 0 =>
      Set(ArcType)
    case 1 =>
      DataType.all
    case _ =>
      Set.empty
  }

  def rightType(pos: Int) = pos match {
    case 0 =>
      DataType.all
    case 1 =>
      Set(ArcType)
    case _ =>
      Set.empty
  }

  override def toString = variable.toString
}

case class ArcPattern(relation: Option[Relation], source: ArcPatternArgument, destinations: List[ArcPatternArgument]) extends RelationalPattern {
  def extend(wordNet: WordNetStore, bindings: Bindings, extensionSet: ExtensionSet, from: Int) = {
    relation.map(wordNet.extend(extensionSet, _, from, source.name, destinations.map(_.name)))
      .getOrElse(wordNet.extend(extensionSet, from, (source.name, source.nodeType), if (destinations == List(ArcPatternArgument("_", None))) Nil else destinations.map(dest => (dest.name, dest.nodeType))))
  }

  def minSize = 2*destinations.size

  def maxSize = Some(2*destinations.size)

  def sourceType = demandArgumentType(source)

  def leftType(pos: Int) = {
    if (pos < minSize)
      if (pos % 2 == 0) {
        Set(ArcType)
      } else {
        demandArgumentType(destinations((pos - 1)/2))
      }
    else
      Set.empty
  }

  def rightType(pos: Int) = {
    if (pos < destinations.size)
      if (pos % 2 == 1) {
        Set(ArcType)
      } else {
        demandArgumentType(destinations(destinations.size - 1 - pos/2))
      }
    else
      Set.empty
  }

  override def toString = (source::relation.map(_.name).getOrElse("_")::destinations).mkString("^")

  private def demandArgumentType(argument: ArcPatternArgument) = {
    relation.map(rel => Set(rel.demandArgument(argument.name).nodeType))
      .getOrElse(argument.nodeType.map(Set(_)).getOrElse(NodeType.all)).asInstanceOf[Set[DataType]]
  }
}

case class ArcPatternArgument(name: String, nodeType: Option[NodeType]) {
  override def toString = name + nodeType.map("&" + _).getOrElse("")
}

case class BindOp(op: AlgebraOp, variables: VariableTemplate) extends QueryOp with VariableBindings with VariableTypeBindings {
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

  def referencedVariables = op.referencedVariables

  def referencesContext = op.referencesContext
}

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

trait VariableBindings {
  def bind(dataSet: DataSet, variables: VariableTemplate) = {
    if (variables != VariableTemplate.empty) {
      val pathVarBuffers = DataSetBuffers.createPathVarBuffers(variables.pathVariableName.map(p => Set(p)).getOrElse(Set.empty))
      val stepVarBuffers = DataSetBuffers.createStepVarBuffers(variables.stepVariableNames)

      variables.pathVariablePosition match {
        case Some(pathVarPos) =>
          val leftVars = variables.leftVariablesIndexes
          val rightVars = variables.rightVariablesIndexes
          val pathVarBuffer = if (variables.variables(pathVarPos).name != "_") Some(pathVarBuffers(variables.variables(pathVarPos).name)) else None
          val pathVarStart = leftVars.size
          val pathVarEnd = rightVars.size

          for (tuple <- dataSet.paths) {
            dataSet.paths.foreach(tuple => bindVariablesFromRight(rightVars, stepVarBuffers, tuple.size))
            pathVarBuffer.map(_.append((pathVarStart, tuple.size - pathVarEnd)))
            dataSet.paths.foreach(tuple => bindVariablesFromLeft(leftVars, stepVarBuffers, tuple.size))
          }
        case None =>
          val rightVars = variables.rightVariablesIndexes
          dataSet.paths.foreach(tuple => bindVariablesFromRight(rightVars, stepVarBuffers, tuple.size))
      }

      DataSet(dataSet.paths, dataSet.pathVars ++ pathVarBuffers.mapValues(_.toList), dataSet.stepVars ++ stepVarBuffers.mapValues(_.toList))
    } else {
      dataSet
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
      Set(relation.demandArgument(args(pos)).nodeType)
    else
      Set.empty
  }

  def minTupleSize = to.size

  def maxTupleSize = Some(to.size)

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

  def maxTupleSize = Some(dataSet.maxTupleSize)

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

  def maxTupleSize = Some(1)

  def bindingsPattern = BindingsPattern()

  def referencedVariables = Set.empty

  def referencesContext = true
}

case class PathVariableRefOp(name: String, types: (AlgebraOp, Int, Int)) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = bindings.lookupPathVariable(name).map(DataSet.fromTuple(_)).get

  def leftType(pos: Int) = types._1.leftType(pos + types._2)

  def rightType(pos: Int) = types._1.rightType(pos + types._3)

  def minTupleSize = types._1.minTupleSize  - types._2

  def maxTupleSize = types._1.maxTupleSize.map(_ - types._3)

  def bindingsPattern = BindingsPattern()

  def referencedVariables = Set(PathVariable(name))

  def referencesContext = false
}

case class StepVariableRefOp(name: String, types: Set[DataType]) extends QueryOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = bindings.lookupStepVariable(name).map(DataSet.fromValue(_)).get

  def leftType(pos: Int) = if (pos == 0) types else Set.empty

  def rightType(pos: Int) = if (pos == 0) types else Set.empty

  def minTupleSize = 1

  def maxTupleSize = Some(1)

  def bindingsPattern = BindingsPattern()

  def referencedVariables = Set(StepVariable(name))

  def referencesContext = false
}
