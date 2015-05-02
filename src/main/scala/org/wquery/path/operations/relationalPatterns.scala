package org.wquery.path.operations

import org.wquery.lang.operations.{Bindings, ProvidesTupleSizes, ProvidesTypes}
import org.wquery.model._
import org.wquery.path._
import org.wquery.utils.IntOptionW._

import scala.collection.mutable.ListBuffer
import scalaz.Scalaz._

sealed abstract class RelationalPattern extends ProvidesTypes with ProvidesTupleSizes {
  def extend(wordNet: WordNet, bindings: Bindings, extensionSet: ExtensionSet): ExtendedExtensionSet

  def sourceType: Set[DataType]

}

case class RelationUnionPattern(patterns: List[RelationalPattern]) extends RelationalPattern {
  def extend(wordNet: WordNet, bindings: Bindings, extensionSet: ExtensionSet) = {
    val buffer = new ExtensionSetBuffer(extensionSet)

    patterns.foreach(expr => buffer.append(expr.extend(wordNet, bindings, extensionSet)))
    buffer.toExtensionSet
  }

  val minTupleSize = patterns.map(_.minTupleSize).min

  val maxTupleSize = if (patterns.exists(!_.maxTupleSize.isDefined)) none else patterns.map(_.maxTupleSize).max

  def sourceType = patterns.flatMap(_.sourceType).toSet

  def leftType(pos: Int) = patterns.flatMap(_.leftType(pos)).toSet

  def rightType(pos: Int) = patterns.flatMap(_.rightType(pos)).toSet

}

case class RelationCompositionPattern(patterns: List[RelationalPattern]) extends RelationalPattern {
  def extend(wordNet: WordNet, bindings: Bindings, extensionSet: ExtensionSet) = {
    val headSet = patterns.head.extend(wordNet, bindings, extensionSet)

    patterns.tail.foldLeft(headSet)((dataSet, expr) => expr.extend(wordNet, bindings, dataSet))
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

}

case class QuantifiedRelationPattern(pattern: RelationalPattern, quantifier: Quantifier) extends RelationalPattern {
  def extend(wordNet: WordNet, bindings: Bindings, extensionSet: ExtensionSet) = {
    val lowerExtensionSet = (1 to quantifier.lowerBound).foldLeft(extensionSet)((x, _) => pattern.extend(wordNet, bindings, x))

    if (some(quantifier.lowerBound) != quantifier.upperBound)
      computeClosureForward(wordNet, bindings, lowerExtensionSet, quantifier.upperBound.map(_ - quantifier.lowerBound))
    else {
      lowerExtensionSet.asInstanceOf[ExtendedExtensionSet] // valid quantifiers invoke extend at least once
    }
  }

  private def computeClosureForward(wordNet: WordNet, bindings: Bindings, extensionSet: ExtensionSet, limit: Option[Int]) = {
    val builder = new ExtensionSetBuilder(extensionSet)

    for (i <- 0 until extensionSet.size) {
      val source = extensionSet.right(i)
      builder.extend(i, Nil)

      for (closureNode <- closeTupleForward(wordNet, bindings, List(source), Set(source), limit))
        builder.extend(i, closureNode)
    }

    builder.build
  }

  private def closeTupleForward(wordNet: WordNet, bindings: Bindings, source: List[Any], forbidden: Set[Any], limit: Option[Int]): Seq[List[Any]] = {
    if ((limit|1) > 0) {
      val transformed = pattern.extend(wordNet, bindings, new DataExtensionSet(DataSet.fromList(source)))
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

}

case class ArcRelationalPattern(pattern: ArcPattern) extends RelationalPattern {
  def extend(wordNet: WordNet, bindings: Bindings, extensionSet: ExtensionSet) = {
    pattern.relations.some(wordNet.extend(extensionSet, _, pattern.source.name, pattern.destination.name))
      .none(wordNet.extend(extensionSet, (pattern.source.name, pattern.source.nodeType),
        (pattern.destination.name, pattern.destination.nodeType)))
  }

  val minTupleSize = 2

  val maxTupleSize = some(2)

  def sourceType = demandArgumentType(pattern.source)

  def leftType(pos: Int) = {
    pos match {
      case 0 =>
        Set(ArcType)
      case 1 =>
        demandArgumentType(pattern.destination)
      case _ =>
        Set.empty
    }
  }

  def rightType(pos: Int) = {
    pos match {
      case 0 =>
        demandArgumentType(pattern.destination)
      case 1 =>
        Set(ArcType)
      case _ =>
        Set.empty
    }
  }

  override def toString = pattern.toString

  private def demandArgumentType(argument: ArcPatternArgument) = {
    pattern.relations.some(_.map(rel => rel.demandArgument(argument.name).nodeType).toSet)
      .none(argument.nodeType.some(Set(_)).none(NodeType.all)).asInstanceOf[Set[DataType]]
  }

}
