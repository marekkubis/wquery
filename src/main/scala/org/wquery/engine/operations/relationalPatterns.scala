package org.wquery.engine.operations

import scalaz._
import Scalaz._
import org.wquery.model._
import collection.mutable.ListBuffer
import org.wquery.WQueryEvaluationException
import org.wquery.engine._

sealed abstract class RelationalPattern {
  def extend(wordNet: WordNetStore, bindings: Bindings, extensionSet: ExtensionSet, from: Int, direction: Direction): ExtendedExtensionSet

  def fringe(wordNet: WordNetStore, bindings: Bindings, side: Side): DataSet

  def minSize: Int

  def maxSize: Option[Int]

  def sourceType: Set[DataType]

  def leftType(pos: Int): Set[DataType]

  def rightType(pos: Int): Set[DataType]

  def maxCount(pathCount: Option[BigInt], wordNet: WordNetSchema): Option[BigInt]

  def fringeMaxCount(side: Side, wordNet: WordNetSchema): BigInt
}

case class RelationUnionPattern(patterns: List[RelationalPattern]) extends RelationalPattern {
  def extend(wordNet: WordNetStore, bindings: Bindings, extensionSet: ExtensionSet, from: Int, direction: Direction) = {
    val buffer = new ExtensionSetBuffer(extensionSet, direction)

    patterns.foreach(expr => buffer.append(expr.extend(wordNet, bindings, extensionSet, from, direction)))
    buffer.toExtensionSet
  }

  def fringe(wordNet: WordNetStore, bindings: Bindings, side: Side) = {
    val buffer = new DataSetBuffer

    patterns.foreach(expr => buffer.append(expr.fringe(wordNet, bindings, side)))
    buffer.toDataSet.distinct
  }

  def minSize = patterns.map(_.minSize).min

  def maxSize = if (patterns.exists(!_.maxSize.isDefined)) none else patterns.map(_.maxSize).max

  def sourceType = patterns.flatMap(_.sourceType).toSet

  def leftType(pos: Int) = patterns.flatMap(_.leftType(pos)).toSet

  def rightType(pos: Int) = patterns.flatMap(_.rightType(pos)).toSet

  def maxCount(pathCount: Option[BigInt], wordNet: WordNetSchema) = patterns.map(_.maxCount(pathCount, wordNet)).sequence.map(_.sum)

  def fringeMaxCount(side: Side, wordNet: WordNetSchema) = patterns.map(_.fringeMaxCount(side, wordNet)).sum
}

case class RelationCompositionPattern(patterns: List[RelationalPattern]) extends RelationalPattern {
  def extend(wordNet: WordNetStore, bindings: Bindings, extensionSet: ExtensionSet, from: Int, direction: Direction) = {
    direction match {
      case Forward =>
        val headSet = patterns.head.extend(wordNet, bindings, extensionSet, from, direction)

        patterns.tail.foldLeft(headSet)((dataSet, expr) => expr.extend(wordNet, bindings, dataSet, 0, direction))
      case Backward =>
        val tailSet = patterns.tail.foldRight(extensionSet){ case (expr, dataSet) =>
          expr.extend(wordNet, bindings, dataSet, 0, direction)
        }

        patterns.head.extend(wordNet, bindings, tailSet, from, direction)
    }
  }

  def fringe(wordNet: WordNetStore, bindings: Bindings, side: Side) = {
    side match {
      case Left =>
        patterns.head.fringe(wordNet, bindings, Left)
      case Right =>
        patterns.last.fringe(wordNet, bindings, Right)
    }
  }

  def minSize = patterns.map(_.minSize).sum

  def maxSize = patterns.map(_.maxSize).sequence.map(_.sum)

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
    }.orZero
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
    }.orZero
  }

  def maxCount(pathCount: Option[BigInt], wordNet: WordNetSchema) = {
    patterns.tail.foldLeft(patterns.head.maxCount(pathCount, wordNet))((pathCount, pattern) => pattern.maxCount(pathCount, wordNet))
  }

  def fringeMaxCount(side: Side, wordNet: WordNetSchema) = {
    side match {
      case Left =>
        patterns.head.fringeMaxCount(Left, wordNet)
      case Right =>
        patterns.last.fringeMaxCount(Right, wordNet)
    }
  }
}

case class QuantifiedRelationPattern(pattern: RelationalPattern, quantifier: Quantifier) extends RelationalPattern {
  def extend(wordNet: WordNetStore, bindings: Bindings, extensionSet: ExtensionSet, from: Int, direction: Direction) = {
    val lowerExtensionSet = (1 to quantifier.lowerBound).foldLeft(extensionSet)((x, _) => pattern.extend(wordNet, bindings, x, from, direction))

    if (some(quantifier.lowerBound) != quantifier.upperBound)
      direction match {
        case Forward =>
          computeClosureForward(wordNet, bindings, lowerExtensionSet, from, direction, quantifier.upperBound.map(_ - quantifier.lowerBound))
        case Backward =>
          computeClosureBackward(wordNet, bindings, lowerExtensionSet, direction, quantifier.upperBound.map(_ - quantifier.lowerBound))
      }
    else {
      lowerExtensionSet.asInstanceOf[ExtendedExtensionSet] // valid quantifiers invoke extend at least once
    }
  }

  def fringe(wordNet: WordNetStore, bindings: Bindings, side: Side) = {
    if (quantifier.lowerBound > 0)
      pattern.fringe(wordNet, bindings, side)
    else {
      val dataTypes = getFringeTypes(side)

      val buffer = new DataSetBuffer

      if (DataType.domain.exists(dataTypes.contains(_)))
        buffer.append(wordNet.fringe(getFringeDomainRelations(dataTypes), distinct = false))

      if (!dataTypes.subsetOf(DataType.domain))
        buffer.append(pattern.fringe(wordNet, bindings, side))

      buffer.toDataSet.distinct
    }
  }

  private def computeClosureForward(wordNet: WordNetStore, bindings: Bindings, extensionSet: ExtensionSet, from: Int, direction: Direction, limit: Option[Int]) = {
    val builder = new ExtensionSetBuilder(extensionSet, direction)

    for (i <- 0 until extensionSet.size) {
      val source = extensionSet.right(i, from)
      builder.extend(i, Nil)

      for (cnode <- closeTupleForward(wordNet, bindings, List(source), Set(source), limit))
        builder.extend(i, cnode)
    }

    builder.build
  }

  private def computeClosureBackward(wordNet: WordNetStore, bindings: Bindings, extensionSet: ExtensionSet, direction: Direction, limit: Option[Int]) = {
    val builder = new ExtensionSetBuilder(extensionSet, direction)

    for (i <- 0 until extensionSet.size) {
      val source = extensionSet.left(i, 0)
      builder.extend(i, Nil)

      for (cnode <- closeTupleBackward(wordNet, bindings, List(source), Set(source), limit))
        builder.extend(i, cnode)
    }

    builder.build
  }

  private def closeTupleForward(wordNet: WordNetStore, bindings: Bindings, source: List[Any], forbidden: Set[Any], limit: Option[Int]): Seq[List[Any]] = {
    if (limit.getOrElse(1) > 0) {
      val transformed = pattern.extend(wordNet, bindings, new DataExtensionSet(DataSet.fromList(source)), 0, Forward)
      val filtered = transformed.extensions.filter{ case (_, extension) => !forbidden.contains(extension.last)}.map(_._2)

      if (filtered.isEmpty) {
        filtered
      } else {
        val result = new ListBuffer[List[Any]]
        val newForbidden = forbidden ++ filtered.map(_.last)
        val newLimit = limit.map(_ - 1)

        for (extension <- filtered) {
          val newSource = source ++ extension

          result.append(newSource.tail)
          result.appendAll(closeTupleForward(wordNet, bindings, newSource, newForbidden, newLimit))
        }

        result
      }
    } else {
      Nil
    }
  }

  private def closeTupleBackward(wordNet: WordNetStore, bindings: Bindings, source: List[Any], forbidden: Set[Any], limit: Option[Int]): Seq[List[Any]] = {
    if (limit.getOrElse(1) > 0) {
      val transformed = pattern.extend(wordNet, bindings, new DataExtensionSet(DataSet.fromList(source)), 0, Backward)
      val filtered = transformed.extensions.filter{ case (_, extension) => !forbidden.contains(extension.head)}.map(_._2)

      if (filtered.isEmpty) {
        filtered
      } else {
        val result = new ListBuffer[List[Any]]
        val newForbidden = forbidden ++ filtered.map(_.head)
        val newLimit = limit.map(_ - 1)

        for (extension <- filtered) {
          val newSource = extension ++ source

          result.append(newSource.dropRight(1))
          result.appendAll(closeTupleBackward(wordNet, bindings, newSource, newForbidden, newLimit))
        }

        result
      }
    } else {
      Nil
    }
  }

  def minSize = pattern.minSize * quantifier.lowerBound

  def maxSize = (pattern.maxSize |@| quantifier.upperBound)(_ * _)

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

  def maxCount(pathCount: Option[BigInt], wordNet: WordNetSchema) = {
    (1 to wordNet.stats.maxPathSize).foldLeft(pathCount)((count, _) => pattern.maxCount(count, wordNet))
  }

  def fringeMaxCount(side: Side, wordNet: WordNetSchema) = {
    if (quantifier.lowerBound > 0)
      pattern.fringeMaxCount(side, wordNet)
    else {
      val fringeTypes = getFringeTypes(side)
      val domainRelationsMaxCount = (for ((relation, argument) <- getFringeDomainRelations(fringeTypes))
        yield wordNet.stats.fetchMaxCount(relation, List((argument, Nil)), List(argument))).sum

      if (fringeTypes.subsetOf(DataType.domain)) 
        domainRelationsMaxCount 
      else 
        pattern.fringeMaxCount(side, wordNet) + domainRelationsMaxCount
    }
  }

  private def getFringeTypes(side: Side) = side match {
    case Left =>
      leftType(0)
    case Right =>
      rightType(0)
  }

  private def getFringeDomainRelations(dataTypes: Set[DataType]) = {
    dataTypes.map(WordNet.dataTypesRelations.get(_)).flatten.map((_, Relation.Source)).toList
  }
}

case class ArcRelationalPattern(pattern: ArcPattern) extends RelationalPattern {
  def extend(wordNet: WordNetStore, bindings: Bindings, extensionSet: ExtensionSet, from: Int, direction: Direction) = {
    pattern.relation.map(wordNet.extend(extensionSet, _, from, direction, pattern.source.name, pattern.destinations.map(_.name)))
      .getOrElse(wordNet.extend(extensionSet, from, direction, (pattern.source.name, pattern.source.nodeType),
        if (pattern.destinations == List(ArcPatternArgument.Any)) Nil else pattern.destinations.map(dest => (dest.name, dest.nodeType))))
  }

  def fringe(wordNet: WordNetStore, bindings: Bindings, side: Side) = {
    wordNet.fringe(createFringeDescription(wordNet.relations, side))
  }

  def minSize = 2*pattern.destinations.size

  def maxSize = some(2*pattern.destinations.size)

  def sourceType = demandArgumentType(pattern.source)

  def leftType(pos: Int) = {
    if (pos < minSize)
      if (pos % 2 == 0) {
        Set(ArcType)
      } else {
        demandArgumentType(pattern.destinations((pos - 1)/2))
      }
    else
      Set.empty
  }

  def rightType(pos: Int) = {
    if (pos < pattern.destinations.size)
      if (pos % 2 == 1) {
        Set(ArcType)
      } else {
        demandArgumentType(pattern.destinations(pattern.destinations.size - 1 - pos/2))
      }
    else
      Set.empty
  }

  override def toString = pattern.toString

  private def demandArgumentType(argument: ArcPatternArgument) = {
    pattern.relation.map(rel => Set(rel.demandArgument(argument.name).nodeType))
      .getOrElse(argument.nodeType.map(Set(_)).getOrElse(NodeType.all)).asInstanceOf[Set[DataType]]
  }

  def maxCount(pathCount: Option[BigInt], wordNet: WordNetSchema) = wordNet.stats.extendMaxCount(pathCount, pattern)

  def fringeMaxCount(side: Side, wordNet: WordNetSchema) = {
    (for ((relation, argument) <- createFringeDescription(wordNet.relations, side))
    yield wordNet.stats.fetchMaxCount(relation, List((argument, Nil)), List(argument))).sum
  }

  private def createFringeDescription(relations: List[Relation], side: Side) = {
    val (fringeName, fringeType) = side match {
      case Left =>
        (pattern.source.name, pattern.source.nodeType)
      case Right =>
        (pattern.destinations.last.name, pattern.destinations.last.nodeType)
    }

    pattern.relation
      .some(rel => List((rel, fringeName)))
      .none(for (relation <- relations if relation.getArgument(fringeName)
      .some(arg => fringeType.some(_ == arg.nodeType).none(true))
      .none(false)) yield (relation, fringeName))
  }
}

case class VariableRelationalPattern(variable: StepVariable) extends RelationalPattern {
  def extend(wordNet: WordNetStore, bindings: Bindings, extensionSet: ExtensionSet, from: Int, direction: Direction) = {
    bindings.lookupStepVariable(variable.name).map {
      case Arc(relation, source, destination) =>
        wordNet.extend(extensionSet, relation, from, direction, source, List(destination))
      case _ =>
        throw new WQueryEvaluationException("Cannot extend a path using a non-arc value of variable " + variable)
    }.getOrElse(throw new WQueryEvaluationException("Variable " + variable + " is not bound"))
  }

  def fringe(wordNet: WordNetStore, bindings: Bindings, side: Side) = {
    bindings.lookupStepVariable(variable.name).map {
      case Arc(relation, source, destination) =>
        side match {
          case Left =>
            wordNet.fringe(List((relation, source)))
          case Right =>
            wordNet.fringe(List((relation, destination)))
        }
      case _ =>
        throw new WQueryEvaluationException("Cannot extend a path using a non-arc value of variable " + variable)
    }.getOrElse(throw new WQueryEvaluationException("Variable " + variable + " is not bound"))
  }

  def minSize = 2

  def maxSize = some(2)

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

  def maxCount(pathCount: Option[BigInt], wordNet: WordNetSchema) = {
    wordNet.stats.extendMaxCount(pathCount, ArcPattern(None, ArcPatternArgument.Any, List(ArcPatternArgument.Any)))
  }

  def fringeMaxCount(side: Side, wordNet: WordNetSchema) = {
    (for (relation <- wordNet.relations; argument <- relation.argumentNames)
      yield wordNet.stats.fetchMaxCount(relation, List((argument, Nil)), List(argument))).sum
  }

  override def toString = variable.toString
}
