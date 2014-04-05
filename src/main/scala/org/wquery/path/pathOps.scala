package org.wquery.path.operations

import org.wquery.model._
import scalaz._
import Scalaz._
import org.wquery.engine._
import org.wquery.engine.operations._
import org.wquery.utils.BigIntOptionW._
import org.wquery.utils.IntOptionW._

sealed abstract class PathOp extends AlgebraOp

/*
 * Set operations
 */
case class UnionOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends PathOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(leftOp.evaluate(wordNet, bindings, context).paths union rightOp.evaluate(wordNet, bindings, context).paths)
  }

  def leftType(pos: Int) = leftOp.leftType(pos) ++ rightOp.leftType(pos)

  def rightType(pos: Int) = leftOp.rightType(pos) ++ rightOp.rightType(pos)

  val minTupleSize = leftOp.minTupleSize min rightOp.minTupleSize

  val maxTupleSize = leftOp.maxTupleSize max rightOp.maxTupleSize

  def bindingsPattern = BindingsPattern()

  val referencedVariables = leftOp.referencedVariables ++ rightOp.referencedVariables

}

case class ExceptOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends PathOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = {
    val leftSet = leftOp.evaluate(wordNet, bindings, context)
    val rightSet = rightOp.evaluate(wordNet, bindings, context)

    DataSet(leftSet.paths.filterNot(rightSet.paths.contains))
  }

  def leftType(pos: Int) = leftOp.leftType(pos)

  def rightType(pos: Int) = leftOp.rightType(pos)

  val minTupleSize = leftOp.minTupleSize

  val maxTupleSize = leftOp.maxTupleSize

  def bindingsPattern = BindingsPattern()

  val referencedVariables = leftOp.referencedVariables ++ rightOp.referencedVariables

}

case class IntersectOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends PathOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(leftOp.evaluate(wordNet, bindings, context).paths intersect rightOp.evaluate(wordNet, bindings, context).paths)
  }

  def leftType(pos: Int) = leftOp.leftType(pos) intersect rightOp.leftType(pos)

  def rightType(pos: Int) = leftOp.rightType(pos) intersect rightOp.rightType(pos)

  val minTupleSize = leftOp.minTupleSize max rightOp.minTupleSize

  val maxTupleSize = leftOp.maxTupleSize min rightOp.maxTupleSize

  def bindingsPattern = BindingsPattern()

  val referencedVariables = leftOp.referencedVariables ++ rightOp.referencedVariables

}

case class JoinOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends PathOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = {
    val leftSet = leftOp.evaluate(wordNet, bindings, context)
    val rightSet = rightOp.evaluate(wordNet, bindings, context)
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
    } else if (leftMaxSize.some(pos < _).none(true)) { // pos < leftMaxSize or leftMaxSize undefined
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
    } else if (rightMaxSize.some(pos < _).none(true)) { // pos < rightMaxSize or rightMaxSize undefined
      val leftOpTypes = for (i <- 0 to pos - rightMinSize) yield leftOp.rightType(i)

      (leftOpTypes :+ rightOp.rightType(pos)).flatten.toSet
    } else { // rightMaxSize defined and pos >= rightMaxSize
      (for (i <- rightMinSize to rightMaxSize.get) yield leftOp.rightType(pos - i)).flatten.toSet
    }
  }

  val minTupleSize = leftOp.minTupleSize + rightOp.minTupleSize

  val maxTupleSize = leftOp.maxTupleSize + rightOp.maxTupleSize

  def bindingsPattern = leftOp.bindingsPattern union rightOp.bindingsPattern

  val referencedVariables = leftOp.referencedVariables ++ rightOp.referencedVariables

}

/*
 * Arithmetic operations
 */
abstract class BinaryArithmeticOp(leftOp: AlgebraOp, rightOp: AlgebraOp) extends PathOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = {
    val leftSet = leftOp.evaluate(wordNet, bindings, context).paths.map(_.last)
    val rightSet = rightOp.evaluate(wordNet, bindings, context).paths.map(_.last)

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
  def combine(leftVal: Any, rightVal: Any): Any = (leftVal, rightVal) match {
    case (leftVal: Double, rightVal: Double) =>
      (leftVal - leftVal % rightVal) / rightVal
    case (leftVal: Double, rightVal: Int) =>
      (leftVal - leftVal % rightVal) / rightVal
    case (leftVal: Int, rightVal: Double) =>
      (leftVal - leftVal % rightVal) / rightVal
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

case class MinusOp(op: AlgebraOp) extends PathOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(op.evaluate(wordNet, bindings, context).paths.map(_.last).map {
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

}

case class FunctionOp(function: Function, args: AlgebraOp) extends PathOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = function.evaluate(args, wordNet, bindings, context)

  def leftType(pos: Int) = function.leftType(args, pos)

  def rightType(pos: Int) = function.rightType(args, pos)

  val minTupleSize = function.minTupleSize(args)

  val maxTupleSize = function.maxTupleSize(args)

  def bindingsPattern = function.bindingsPattern(args)

  val referencedVariables = args.referencedVariables

}

/*
 * Declarative operations
 */
case class SelectOp(op: AlgebraOp, condition: Condition) extends PathOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = {
    val dataSet = op.evaluate(wordNet, bindings, context)
    val pathVarNames = dataSet.pathVars.keySet
    val filterPathVarNames = pathVarNames.filter(pathVarName => condition.referencedVariables.contains(TupleVariable(pathVarName)))
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
        binds.bindTupleVariable(pathVar, tuple.slice(varPos._1, varPos._2))
      }

      for (stepVar <- filterStepVarNames) {
        val varPos = dataSet.stepVars(stepVar)(i)
        binds.bindStepVariable(stepVar, tuple(varPos))
      }

      if (condition.satisfied(wordNet, binds, context)) {
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

}

case class ProjectOp(op: AlgebraOp, projectOp: AlgebraOp) extends PathOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = {
    val dataSet = op.evaluate(wordNet, bindings, context)
    val buffer = new DataSetBuffer
    val pathVarNames = dataSet.pathVars.keys
    val filterPathVarNames = pathVarNames.filter(pathVarName => projectOp.referencedVariables.contains(TupleVariable(pathVarName)))
    val stepVarNames = dataSet.stepVars.keys
    val filterStepVarNames = stepVarNames.filter(stepVarName => projectOp.referencedVariables.contains(StepVariable(stepVarName)))
    val binds = Bindings(bindings, false)

    for (i <- 0 until dataSet.pathCount) {
      val tuple = dataSet.paths(i)

      for (pathVar <- filterPathVarNames) {
        val varPos = dataSet.pathVars(pathVar)(i)
        binds.bindTupleVariable(pathVar, tuple.slice(varPos._1, varPos._2))
      }

      for (stepVar <- filterStepVarNames) {
        val varPos = dataSet.stepVars(stepVar)(i)
        binds.bindStepVariable(stepVar, tuple(varPos))
      }

      buffer.append(projectOp.evaluate(wordNet, binds, context))
    }

    buffer.toDataSet
  }

  def leftType(pos: Int) = projectOp.leftType(pos)

  def rightType(pos: Int) = projectOp.rightType(pos)

  val minTupleSize = projectOp.minTupleSize

  val maxTupleSize = projectOp.maxTupleSize

  def bindingsPattern = projectOp.bindingsPattern

  val referencedVariables = op.referencedVariables ++ (projectOp.referencedVariables -- op.bindingsPattern.variables)

}

case class ExtendOp(op: AlgebraOp, pattern: RelationalPattern, variables: VariableTemplate) extends PathOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = {
    val dataSet = op.evaluate(wordNet, bindings, context)
    val extensionSet = pattern.extend(wordNet, bindings, new DataExtensionSet(dataSet))
    val dataSetPathVarNames = dataSet.pathVars.keySet
    val dataSetStepVarNames = dataSet.stepVars.keySet
    val pathBuffer = DataSetBuffers.createPathBuffer
    val pathVarBuffers = DataSetBuffers.createPathVarBuffers(variables.pathVariableName.some(dataSetPathVarNames + _).none(dataSetPathVarNames))
    val stepVarBuffers = DataSetBuffers.createStepVarBuffers(dataSetStepVarNames union variables.stepVariableNames)

    for ((pathPos, extension) <- extensionSet.extensions) {
      val path = dataSet.paths(pathPos)
      val extensionSize = extension.size
      val pathShift = 0
      val extensionShift = path.size

      pathBuffer.append(path ++ extension)

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
    } else if (op.maxTupleSize.some(pos < _).none(true)) { // pos < maxTupleSize or maxTupleSize undefined
      val extendOpTypes = for (i <- 0 to pos - op.minTupleSize) yield pattern.leftType(i)

      (extendOpTypes :+ op.leftType(pos)).flatten.toSet
    } else { // maxTupleSize defined and pos >= maxTupleSize
      (for (i <- op.minTupleSize to op.maxTupleSize.get) yield pattern.leftType(pos - i)).flatten.toSet
    }
  }

  def rightType(pos: Int) = {
    if (pos < pattern.minTupleSize) {
      pattern.rightType(pos)
    } else if (pattern.maxTupleSize.some(pos < _).none(true)) { // pos < maxSize or maxSize undefined
      val extendOpTypes = for (i <- 0 to pos - pattern.minTupleSize) yield op.rightType(i)

      (extendOpTypes :+ pattern.rightType(pos)).flatten.toSet
    } else { // maxSize defined and pos >= maxSize
      (for (i <- pattern.minTupleSize to pattern.maxTupleSize.get) yield op.rightType(pos - i)).flatten.toSet
    }
  }

  val minTupleSize = op.minTupleSize + pattern.minTupleSize

  val maxTupleSize = op.maxTupleSize + pattern.maxTupleSize

  def bindingsPattern = {
    val pattern = op.bindingsPattern
    pattern.bindVariablesTypes(variables, this)
    pattern
  }

  val referencedVariables = op.referencedVariables

}

case class BindOp(op: AlgebraOp, variables: VariableTemplate) extends PathOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = {
    op.evaluate(wordNet, bindings, context).bindVariables(variables)
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

}

/*
 * Elementary operations
 */
case class SynsetFetchOp(op: AlgebraOp) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = {
   if (context.creation) {
     val senses = op.evaluate(wordNet, bindings, context).paths.map(_.last.asInstanceOf[Sense])

     if (!senses.isEmpty) {
       val synset = wordNet.getSynset(senses.head).some { synset =>
         if (wordNet.getSenses(synset) == senses)
           synset
         else
           new NewSynset(senses)
       }.none(new NewSynset(senses))

       DataSet.fromValue(synset)
     } else {
       DataSet.empty
     }
   } else {
     val contextTypes = op.rightType(0).filter(t => t === StringType || t === SenseType)
     val patterns = contextTypes.map{ contextType => (contextType: @unchecked) match {
       case StringType =>
         ArcRelationalPattern(ArcPattern(Some(WordNet.WordFormToSynsets), ArcPatternArgument(Relation.Src, Some(WordNet.WordFormToSynsets.sourceType)),
           List(ArcPatternArgument(Relation.Dst, WordNet.WordFormToSynsets.destinationType))))
       case SenseType =>
         ArcRelationalPattern(ArcPattern(Some(WordNet.SenseToSynset), ArcPatternArgument(Relation.Src, Some(WordNet.SenseToSynset.sourceType)),
           List(ArcPatternArgument(Relation.Dst, WordNet.SenseToSynset.destinationType))))
     }}.toList

     val projectedVariable = StepVariable("__p")
     ProjectOp(ExtendOp(op, RelationUnionPattern(patterns), VariableTemplate(List(projectedVariable))),
       StepVariableRefOp(projectedVariable, Set(SynsetType))).evaluate(wordNet, bindings, context)
   }

  }

  def leftType(pos: Int) = if (pos == 0) Set(SynsetType) else Set.empty

  def rightType(pos: Int) = if (pos == 0) Set(SynsetType) else Set.empty

  val minTupleSize = 1

  val maxTupleSize = some(1)

  def bindingsPattern = BindingsPattern()

  val referencedVariables = op.referencedVariables

}

class NewSynset(val senses: List[Sense]) extends Synset("synset#" + senses.head.toString)

case class FetchOp(relation: Relation, from: List[(String, List[Any])], to: List[String], withArcs: Boolean = true) extends PathOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = {
    if (context.creation && WordNet.domainRelations.contains(relation) && from.forall { case (source, values) => values.nonEmpty }) {
      DataSet(from.flatMap{ case (_, value) => List(value) })
    } else {
      wordNet.fetch(relation, from, to, withArcs)
    }
  }

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

}

object FetchOp {
  val words = FetchOp(WordNet.WordSet, List((Relation.Src, Nil)), List(Relation.Src))

  val senses = FetchOp(WordNet.SenseSet, List((Relation.Src, Nil)), List(Relation.Src))

  val synsets = FetchOp(WordNet.SynsetSet, List((Relation.Src, Nil)), List(Relation.Src))

  val possyms = FetchOp(WordNet.PosSet, List((Relation.Src, Nil)), List(Relation.Src))

  def wordByValue(value: String)
    = FetchOp(WordNet.WordSet, List((Relation.Src, List(value))), List(Relation.Src))

  def senseByValue(sense: Sense) = {
    FetchOp(WordNet.SenseSet,
      List((Relation.Src, List(sense))), List(Relation.Src))
  }

  def sensesByWordFormAndSenseNumber(word: String, num: Int) = {
    FetchOp(WordNet.SenseToWordFormSenseNumberAndPos,
      List((Relation.Dst, List(word)), ("num", List(num))), List(Relation.Src))
  }
}

case class ConstantOp(dataSet: DataSet) extends PathOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = dataSet

  def leftType(pos: Int) = dataSet.leftType(pos)

  def rightType(pos: Int) = dataSet.rightType(pos)

  val minTupleSize = dataSet.minTupleSize

  val maxTupleSize = dataSet.maxTupleSize

  def bindingsPattern = BindingsPattern() // assumed that a constant dataset does not contain variable bindings

  val referencedVariables = ∅[Set[Variable]]

}

object ConstantOp {
  def fromValue(value: Any) = ConstantOp(DataSet.fromValue(value))

  val empty = ConstantOp(DataSet.empty)
}

/*
 * Reference operations
 */
case class SetVariableRefOp(variable: SetVariable, op: AlgebraOp) extends PathOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = bindings.demandSetVariable(variable.name)

  def leftType(pos: Int) = op.leftType(pos)

  def rightType(pos: Int) = op.rightType(pos)

  val minTupleSize = op.minTupleSize

  val maxTupleSize = op.maxTupleSize

  def bindingsPattern = BindingsPattern()

  val referencedVariables = Set[Variable](variable)

}

case class PathVariableRefOp(variable: TupleVariable, types: (AlgebraOp, Int, Int)) extends PathOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = DataSet.fromTuple(bindings.demandTupleVariable(variable.name))

  def leftType(pos: Int) = types._1.leftType(pos + types._2)

  def rightType(pos: Int) = types._1.rightType(pos + types._3)

  val minTupleSize = types._1.minTupleSize  - types._2

  val maxTupleSize = types._1.maxTupleSize.map(_ - types._3)

  def bindingsPattern = BindingsPattern()

  val referencedVariables = Set[Variable](variable)

}

case class StepVariableRefOp(variable: StepVariable, types: Set[DataType]) extends PathOp {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = DataSet.fromValue(bindings.demandStepVariable(variable.name))

  def leftType(pos: Int) = if (pos == 0) types else Set.empty

  def rightType(pos: Int) = if (pos == 0) types else Set.empty

  val minTupleSize = 1

  val maxTupleSize = some(1)

  def bindingsPattern = BindingsPattern()

  val referencedVariables = Set[Variable](variable)

}
