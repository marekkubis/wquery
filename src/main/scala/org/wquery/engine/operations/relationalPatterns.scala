package org.wquery.engine.operations

import scalaz._
import Scalaz._
import org.wquery.model._
import collection.mutable.ListBuffer
import org.wquery.engine._
import org.wquery.utils.IntOptionW._

sealed abstract class RelationalPattern extends ProvidesTypes with ProvidesTupleSizes {
  def extend(wordNet: WordNet, bindings: Bindings, extensionSet: ExtensionSet, direction: Direction): ExtendedExtensionSet

  def fringe(wordNet: WordNet, bindings: Bindings, side: Side): DataSet

  def sourceType: Set[DataType]

  def maxCount(pathCount: Option[BigInt], wordNet: WordNetSchema, direction: Direction): Option[BigInt]

  def fringeMaxCount(side: Side, wordNet: WordNetSchema): BigInt
}

case class RelationUnionPattern(patterns: List[RelationalPattern]) extends RelationalPattern {
  def extend(wordNet: WordNet, bindings: Bindings, extensionSet: ExtensionSet, direction: Direction) = {
    val buffer = new ExtensionSetBuffer(extensionSet, direction)

    patterns.foreach(expr => buffer.append(expr.extend(wordNet, bindings, extensionSet, direction)))
    buffer.toExtensionSet
  }

  def fringe(wordNet: WordNet, bindings: Bindings, side: Side) = {
    val buffer = new DataSetBuffer

    patterns.foreach(expr => buffer.append(expr.fringe(wordNet, bindings, side)))
    buffer.toDataSet.distinct
  }

  val minTupleSize = patterns.map(_.minTupleSize).min

  val maxTupleSize = if (patterns.exists(!_.maxTupleSize.isDefined)) none else patterns.map(_.maxTupleSize).max

  def sourceType = patterns.flatMap(_.sourceType).toSet

  def leftType(pos: Int) = patterns.flatMap(_.leftType(pos)).toSet

  def rightType(pos: Int) = patterns.flatMap(_.rightType(pos)).toSet

  def maxCount(pathCount: Option[BigInt], wordNet: WordNetSchema, direction: Direction) = {
    patterns.map(_.maxCount(pathCount, wordNet, direction)).sequence.map(_.sum)
  }

  def fringeMaxCount(side: Side, wordNet: WordNetSchema) = patterns.map(_.fringeMaxCount(side, wordNet)).sum
}

case class RelationCompositionPattern(patterns: List[RelationalPattern]) extends RelationalPattern {
  def extend(wordNet: WordNet, bindings: Bindings, extensionSet: ExtensionSet, direction: Direction) = {
    direction match {
      case Forward =>
        val headSet = patterns.head.extend(wordNet, bindings, extensionSet, direction)

        patterns.tail.foldLeft(headSet)((dataSet, expr) => expr.extend(wordNet, bindings, dataSet, direction))
      case Backward =>
        val tailSet = patterns.tail.foldRight(extensionSet){ case (expr, dataSet) =>
          expr.extend(wordNet, bindings, dataSet, direction)
        }

        patterns.head.extend(wordNet, bindings, tailSet, direction)
    }
  }

  def fringe(wordNet: WordNet, bindings: Bindings, side: Side) = {
    side match {
      case Left =>
        patterns.head.fringe(wordNet, bindings, Left)
      case Right =>
        patterns.last.fringe(wordNet, bindings, Right)
    }
  }

  val minTupleSize = patterns.map(_.minTupleSize).sum

  val maxTupleSize = patterns.map(_.maxTupleSize).sequence.map(_.sum)

  def sourceType = patterns.head.sourceType

  def leftType(pos: Int) = leftType(patterns, pos)

  private def leftType(patterns: List[RelationalPattern], pos: Int): Set[DataType] = {
    patterns.headOption.map { headPattern =>
      if (pos < headPattern.minTupleSize)
        headPattern.leftType(pos)
      else if (headPattern.maxTupleSize.some(pos < _).none(true)) { // pos < maxSize or maxSize undefined
        headPattern.leftType(pos) union leftType(patterns.tail, pos - headPattern.minTupleSize)
      } else { // else pos < maxSize and defined
        leftType(patterns.tail, pos - headPattern.maxTupleSize.get)
      }
    }.orZero
  }

  def rightType(pos: Int) = rightType(patterns.reverse, pos)

  private def rightType(patterns: List[RelationalPattern], pos: Int): Set[DataType] = {
    patterns.headOption.map { headPattern =>
      if (pos < headPattern.minTupleSize)
        headPattern.rightType(pos)
      else if (headPattern.maxTupleSize.some(pos < _).none(true)) { // pos < maxSize or maxSize undefined
        headPattern.rightType(pos) union rightType(patterns.tail, pos - headPattern.minTupleSize)
      } else { // else pos < maxSize and defined
        rightType(patterns.tail, pos - headPattern.maxTupleSize.get)
      }
    }.orZero
  }

  def maxCount(pathCount: Option[BigInt], wordNet: WordNetSchema, direction: Direction) = {
    direction match {
      case Forward =>
        patterns.tail.
          foldLeft(patterns.head.maxCount(pathCount, wordNet, direction))((pathCount, pattern) => pattern.maxCount(pathCount, wordNet, direction))
      case Backward =>
        patterns.dropRight(1).
          foldRight(patterns.last.maxCount(pathCount, wordNet, direction))((pattern, pathCount) => pattern.maxCount(pathCount, wordNet, direction))
    }
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
  def extend(wordNet: WordNet, bindings: Bindings, extensionSet: ExtensionSet, direction: Direction) = {
    val lowerExtensionSet = (1 to quantifier.lowerBound).foldLeft(extensionSet)((x, _) => pattern.extend(wordNet, bindings, x, direction))

    if (some(quantifier.lowerBound) != quantifier.upperBound)
      direction match {
        case Forward =>
          computeClosureForward(wordNet, bindings, lowerExtensionSet, direction, quantifier.upperBound.map(_ - quantifier.lowerBound))
        case Backward =>
          computeClosureBackward(wordNet, bindings, lowerExtensionSet, direction, quantifier.upperBound.map(_ - quantifier.lowerBound))
      }
    else {
      lowerExtensionSet.asInstanceOf[ExtendedExtensionSet] // valid quantifiers invoke extend at least once
    }
  }

  def fringe(wordNet: WordNet, bindings: Bindings, side: Side) = {
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

  private def computeClosureForward(wordNet: WordNet, bindings: Bindings, extensionSet: ExtensionSet, direction: Direction, limit: Option[Int]) = {
    val builder = new ExtensionSetBuilder(extensionSet, direction)

    for (i <- 0 until extensionSet.size) {
      val source = extensionSet.right(i)
      builder.extend(i, Nil)

      for (closureNode <- closeTupleForward(wordNet, bindings, List(source), Set(source), limit))
        builder.extend(i, closureNode)
    }

    builder.build
  }

  private def computeClosureBackward(wordNet: WordNet, bindings: Bindings, extensionSet: ExtensionSet, direction: Direction, limit: Option[Int]) = {
    val builder = new ExtensionSetBuilder(extensionSet, direction)

    for (i <- 0 until extensionSet.size) {
      val source = extensionSet.left(i, 0)
      builder.extend(i, Nil)

      for (closureNode <- closeTupleBackward(wordNet, bindings, List(source), Set(source), limit))
        builder.extend(i, closureNode)
    }

    builder.build
  }

  private def closeTupleForward(wordNet: WordNet, bindings: Bindings, source: List[Any], forbidden: Set[Any], limit: Option[Int]): Seq[List[Any]] = {
    if ((limit|1) > 0) {
      val transformed = pattern.extend(wordNet, bindings, new DataExtensionSet(DataSet.fromList(source)), Forward)
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

  private def closeTupleBackward(wordNet: WordNet, bindings: Bindings, source: List[Any], forbidden: Set[Any], limit: Option[Int]): Seq[List[Any]] = {
    if ((limit|1) > 0) {
      val transformed = pattern.extend(wordNet, bindings, new DataExtensionSet(DataSet.fromList(source)), Backward)
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

  val minTupleSize = pattern.minTupleSize * quantifier.lowerBound

  val maxTupleSize = pattern.maxTupleSize * quantifier.upperBound

  def sourceType = pattern.sourceType

  def leftType(pos: Int) = if (maxTupleSize.some(pos < _).none(true)) {
    (for (i <- pattern.minTupleSize to pattern.maxTupleSize|(pos + 1))
    yield pattern.leftType(if (i > 0) pos % i else 0)).flatten.toSet
  } else {
    Set[DataType]()
  }

  def rightType(pos: Int) = if (maxTupleSize.some(pos < _).none(true)) {
    (for (i <- pattern.minTupleSize to pattern.maxTupleSize|(pos + 1))
    yield pattern.rightType(if (i > 0) pos % i else 0)).flatten.toSet
  } else {
    Set[DataType]()
  }

  def maxCount(pathCount: Option[BigInt], wordNet: WordNetSchema, direction: Direction) = {
    (1 to wordNet.stats.maxPathSize).foldLeft(pathCount)((count, _) => pattern.maxCount(count, wordNet, direction))
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
  def extend(wordNet: WordNet, bindings: Bindings, extensionSet: ExtensionSet, direction: Direction) = {
    pattern.relation.some(wordNet.extend(extensionSet, _, direction, pattern.source.name, pattern.destinations.map(_.name)))
      .none(wordNet.extend(extensionSet, direction, (pattern.source.name, pattern.source.nodeType),
        if (pattern.destinations == List(ArcPatternArgument.Any)) Nil else pattern.destinations.map(dest => (dest.name, dest.nodeType))))
  }

  def fringe(wordNet: WordNet, bindings: Bindings, side: Side) = {
    wordNet.fringe(createFringeDescription(wordNet.relations, side))
  }

  val minTupleSize = 2*pattern.destinations.size

  val maxTupleSize = some(2*pattern.destinations.size)

  def sourceType = demandArgumentType(pattern.source)

  def leftType(pos: Int) = {
    if (pos < minTupleSize)
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
    pattern.relation.some(rel => Set(rel.demandArgument(argument.name).nodeType))
      .none(argument.nodeType.some(Set(_)).none(NodeType.all)).asInstanceOf[Set[DataType]]
  }

  def maxCount(pathCount: Option[BigInt], wordNet: WordNetSchema, direction: Direction) = wordNet.stats.extendMaxCount(pathCount, pattern, direction)

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
        .none(fringeName == ArcPatternArgument.AnyName)) yield (relation, fringeName))
  }
}
